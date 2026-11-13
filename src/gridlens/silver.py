"""Silver: typed, validated readings. Rows failing a data-quality rule are quarantined, not dropped.

Each bronze row gets the first rule it fails (rules are evaluated in order). Passing rows
are de-duplicated on (household_id, ts); extra copies are quarantined as duplicates.
Results land in three Delta tables:

* ``silver_readings``  - clean rows
* ``silver_rejected``  - every rejected bronze row with its ``dq_rule``
* ``dq_report``        - one row per rule with the rejected count, plus totals
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from gridlens.bronze import BRONZE_TABLE
from gridlens.storage import Lakehouse

SILVER_TABLE = "silver_readings"
REJECTED_TABLE = "silver_rejected"
DQ_REPORT_TABLE = "dq_report"

# A 100 A single-phase domestic supply at 230 V delivers at most 23 kW, i.e. 11.5 kWh
# per half hour. Anything above that is a meter or transcription error.
MAX_KWH_PER_HALF_HOUR = 11.5
VALID_TARIFFS = ("Std", "ToU")


def _strip(col: str) -> Column:
    # F.trim only removes spaces; source values carry padding spaces and, on the last
    # column, can carry a stray carriage return from CRLF line endings.
    return F.regexp_replace(F.col(col), r"^\s+|\s+$", "")


def _parsed(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("household_id", _strip("lcl_id"))
        .withColumn("tariff", _strip("std_or_tou"))
        # Source format: "2012-10-12 00:30:00.0000000" (GMT). try_* functions return NULL
        # instead of raising under ANSI mode (default on Spark 4 and Databricks serverless).
        .withColumn(
            "ts",
            F.try_to_timestamp(F.substring(_strip("datetime_raw"), 1, 19), F.lit("yyyy-MM-dd HH:mm:ss")),
        )
        .withColumn("kwh_str", _strip("kwh_raw"))
        .withColumn("kwh", F.expr("try_cast(kwh_str AS DOUBLE)"))
    )


@dataclass(frozen=True)
class Rule:
    name: str
    description: str
    fails: Column


RULES: list[Rule] = [
    Rule(
        "invalid_household_id",
        "household id missing or not of the form MAC + 6 digits",
        F.col("household_id").isNull() | ~F.col("household_id").rlike(r"^MAC\d{6}$"),
    ),
    Rule(
        "invalid_tariff",
        f"tariff group not one of {', '.join(VALID_TARIFFS)}",
        F.col("tariff").isNull() | ~F.col("tariff").isin(*VALID_TARIFFS),
    ),
    Rule("unparseable_timestamp", "DateTime missing or not yyyy-MM-dd HH:mm:ss", F.col("ts").isNull()),
    Rule(
        "off_interval_timestamp",
        "timestamp not on a half-hour boundary",
        ~F.minute("ts").isin(0, 30) | (F.second("ts") != 0),
    ),
    Rule(
        "missing_kwh",
        "kWh reading empty or the literal 'Null'",
        F.col("kwh_str").isNull() | (F.col("kwh_str") == "") | (F.lower("kwh_str") == "null"),
    ),
    Rule("non_numeric_kwh", "kWh reading is not a number", F.col("kwh").isNull() | F.isnan("kwh")),
    Rule(
        "kwh_out_of_range",
        f"kWh per half hour outside [0, {MAX_KWH_PER_HALF_HOUR}]",
        (F.col("kwh") < 0) | (F.col("kwh") > MAX_KWH_PER_HALF_HOUR),
    ),
]
DUPLICATE_RULE = Rule("duplicate_reading", "second or later reading for the same household and half hour", F.lit(False))
ALL_RULES = [*RULES, DUPLICATE_RULE]


def first_failed_rule() -> Column:
    expr = F.lit(None).cast("string")
    for rule in reversed(RULES):
        expr = F.when(F.coalesce(rule.fails, F.lit(True)), F.lit(rule.name)).otherwise(expr)
    return expr


def apply_quality(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split bronze rows into (clean silver rows, rejected rows with dq_rule)."""
    checked = _parsed(bronze).withColumn("dq_rule", first_failed_rule())
    # Rank duplicates only among rows that passed every other rule.
    dup_order = Window.partitionBy("household_id", "ts", F.col("dq_rule").isNull()).orderBy("source_file", "kwh")
    checked = checked.withColumn(
        "dq_rule",
        F.when(F.col("dq_rule").isNotNull(), F.col("dq_rule")).otherwise(
            F.when(F.row_number().over(dup_order) > 1, F.lit(DUPLICATE_RULE.name))
        ),
    )
    clean = checked.where(F.col("dq_rule").isNull()).select(
        "household_id",
        "tariff",
        "ts",
        F.to_date("ts").alias("reading_date"),
        (F.hour("ts") * 2 + F.floor(F.minute("ts") / 30)).cast("int").alias("slot"),
        "kwh",
        "source_file",
    )
    rejected = checked.where(F.col("dq_rule").isNotNull()).select(
        "lcl_id", "std_or_tou", "datetime_raw", "kwh_raw", "source_file", "dq_rule"
    )
    return clean, rejected


def dq_report(lake: Lakehouse, total: int, passed: int, rejected: DataFrame) -> DataFrame:
    counts = {r["dq_rule"]: r["n"] for r in rejected.groupBy("dq_rule").agg(F.count("*").alias("n")).collect()}
    rows = [(rule.name, rule.description, int(counts.get(rule.name, 0))) for rule in ALL_RULES]
    rows += [("TOTAL_INPUT", "bronze rows checked", total), ("TOTAL_PASSED", "rows written to silver", passed)]
    return lake.spark.createDataFrame(rows, "rule string, description string, rows long").withColumn(
        "pct_of_input", F.round(F.col("rows") / F.lit(max(total, 1)) * 100, 4)
    ).withColumn("checked_at", F.current_timestamp())


class DataQualityError(RuntimeError):
    pass


def build_silver(lake: Lakehouse, max_reject_ratio: float = 0.05) -> dict[str, int]:
    """Validate bronze into silver. Fails the run if more than ``max_reject_ratio`` of rows are rejected."""
    bronze = lake.read(BRONZE_TABLE)
    clean, rejected = apply_quality(bronze)
    lake.write(clean, SILVER_TABLE)
    lake.write(rejected, REJECTED_TABLE)
    total = bronze.count()
    passed = lake.read(SILVER_TABLE).count()
    rejected_tbl = lake.read(REJECTED_TABLE)
    report = dq_report(lake, total, passed, rejected_tbl)
    lake.write(report, DQ_REPORT_TABLE)
    summary = {r["rule"]: int(r["rows"]) for r in lake.read(DQ_REPORT_TABLE).collect()}
    if total and (total - passed) / total > max_reject_ratio:
        raise DataQualityError(
            f"{total - passed} of {total} rows rejected (> {max_reject_ratio:.1%}); see {DQ_REPORT_TABLE}"
        )
    return summary

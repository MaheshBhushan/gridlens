"""Gold: business-level aggregates for load profiles and peak demand.

Households join and leave the trial at different times, so demand is reported both as
an aggregate over the households reporting in that half hour and normalised per
household (mean kWh / kW per reporting household). kW = kWh per half hour * 2.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from gridlens.silver import SILVER_TABLE
from gridlens.storage import Lakehouse

HALFHOURLY_TABLE = "gold_halfhourly_demand"
LOAD_PROFILE_TABLE = "gold_load_profile"
DAILY_PEAK_TABLE = "gold_daily_peak"


def season(month: F.Column) -> F.Column:
    return (
        F.when(month.isin(12, 1, 2), "winter")
        .when(month.isin(3, 4, 5), "spring")
        .when(month.isin(6, 7, 8), "summer")
        .otherwise("autumn")
    )


def slot_label(slot: F.Column) -> F.Column:
    return F.format_string("%02d:%02d", F.floor(slot / 2).cast("int"), ((slot % 2) * 30).cast("int"))


def halfhourly_demand(silver: DataFrame) -> DataFrame:
    return (
        silver.groupBy("ts", "reading_date", "slot")
        .agg(F.countDistinct("household_id").alias("households"), F.sum("kwh").alias("total_kwh"))
        .withColumn("mean_kwh", F.col("total_kwh") / F.col("households"))
        .withColumn("aggregate_kw", F.col("total_kwh") * 2)
        .withColumn("mean_kw_per_household", F.col("mean_kwh") * 2)
    )


def load_profile(silver: DataFrame) -> DataFrame:
    """Average half-hourly consumption shape by tariff group, season and weekday/weekend."""
    dow = F.dayofweek("reading_date")  # 1 = Sunday, 7 = Saturday
    return (
        silver.withColumn("season", season(F.month("reading_date")))
        .withColumn("day_type", F.when(dow.isin(1, 7), "weekend").otherwise("weekday"))
        .groupBy("tariff", "season", "day_type", "slot")
        .agg(
            F.countDistinct("household_id").alias("households"),
            F.count("*").alias("readings"),
            F.avg("kwh").alias("mean_kwh"),
            F.percentile_approx("kwh", 0.5, 1000).alias("p50_kwh"),
            F.percentile_approx("kwh", 0.9, 1000).alias("p90_kwh"),
        )
        .withColumn("slot_label", slot_label(F.col("slot")))
        .withColumn("mean_kw", F.col("mean_kwh") * 2)
    )


def daily_peak(halfhourly: DataFrame) -> DataFrame:
    """One row per day: energy, peak half hour (per household and aggregate) and load factor."""
    by_peak = F.struct(F.col("mean_kw_per_household"), F.col("ts"), F.col("aggregate_kw"))
    return (
        halfhourly.groupBy("reading_date")
        .agg(
            F.count("*").alias("intervals"),
            F.max("households").alias("households"),
            F.sum("total_kwh").alias("total_kwh"),
            F.sum("mean_kwh").alias("kwh_per_household"),
            F.avg("mean_kw_per_household").alias("avg_kw_per_household"),
            F.max(by_peak).alias("peak"),
            F.max("aggregate_kw").alias("peak_aggregate_kw"),
        )
        .select(
            "reading_date",
            "intervals",
            "households",
            "total_kwh",
            "kwh_per_household",
            F.col("peak.mean_kw_per_household").alias("peak_kw_per_household"),
            F.col("peak.ts").alias("peak_ts"),
            slot_label(F.hour("peak.ts") * 2 + F.floor(F.minute("peak.ts") / 30)).alias("peak_slot_label"),
            "peak_aggregate_kw",
            (F.col("avg_kw_per_household") / F.col("peak.mean_kw_per_household")).alias("load_factor"),
        )
    )


def build_gold(lake: Lakehouse) -> dict[str, int]:
    silver = lake.read(SILVER_TABLE)
    lake.write(halfhourly_demand(silver), HALFHOURLY_TABLE)
    halfhourly = lake.read(HALFHOURLY_TABLE)
    lake.write(load_profile(silver), LOAD_PROFILE_TABLE)
    lake.write(daily_peak(halfhourly), DAILY_PEAK_TABLE)
    return {t: lake.read(t).count() for t in (HALFHOURLY_TABLE, LOAD_PROFILE_TABLE, DAILY_PEAK_TABLE)}

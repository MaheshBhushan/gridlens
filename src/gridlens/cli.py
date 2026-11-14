"""Command-line entry point. Same commands locally and as Databricks python_wheel_task steps.

Local:       gridlens all --lake lakehouse --source data/raw
Databricks:  gridlens <stage> --catalog <catalog> --schema gridlens
"""

from __future__ import annotations

import argparse
import json
import sys

from pyspark.sql import functions as F

from gridlens import download
from gridlens.bronze import build_bronze
from gridlens.forecast import METRICS_TABLE, build_forecast
from gridlens.gold import DAILY_PEAK_TABLE, LOAD_PROFILE_TABLE, build_gold
from gridlens.silver import DQ_REPORT_TABLE, build_silver
from gridlens.spark import get_spark
from gridlens.storage import Lakehouse
from gridlens.views import create_views

STAGES = ["download", "bronze", "silver", "gold", "forecast", "views", "report", "all"]


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="gridlens", description=__doc__.splitlines()[0])
    p.add_argument("stage", choices=STAGES)
    where = p.add_mutually_exclusive_group(required=True)
    where.add_argument("--lake", help="local directory holding the Delta tables")
    where.add_argument("--catalog", help="Unity Catalog catalog (Databricks)")
    p.add_argument("--schema", default="gridlens", help="Unity Catalog schema (Databricks)")
    p.add_argument("--source", help="raw CSV directory (default: <lake>/../data/raw, or the raw volume on Databricks)")
    p.add_argument("--parts", type=int, default=2, help="download: number of 1M-row source files")
    p.add_argument("--max-reject-ratio", type=float, default=0.05)
    p.add_argument("--min-households", type=int, default=20)
    p.add_argument("--holdout-days", type=int, default=28)
    return p.parse_args(argv)


def report(lake: Lakehouse) -> dict:
    dq = {r["rule"]: r["rows"] for r in lake.read(DQ_REPORT_TABLE).collect()}
    metrics = [
        {k: (round(v, 4) if isinstance(v, float) else str(v) if k.startswith("period") else v)
         for k, v in r.asDict().items()}
        for r in lake.read(METRICS_TABLE).orderBy("split", "model").collect()
    ]
    peaks = lake.read(DAILY_PEAK_TABLE).where("intervals = 48")
    top = peaks.orderBy(F.desc("peak_kw_per_household")).limit(5).collect()
    profile = lake.read(LOAD_PROFILE_TABLE)
    profile_peak = (
        profile.groupBy("tariff", "season", "day_type")
        .agg(F.max(F.struct("mean_kw", "slot_label")).alias("p"), F.max("households").alias("households"))
        .orderBy("tariff", "season", "day_type")
        .collect()
    )
    return {
        "data_quality": dq,
        "complete_days": peaks.count(),
        "top_peak_days": [
            {"date": str(r["reading_date"]), "households": r["households"],
             "peak_kw_per_household": round(r["peak_kw_per_household"], 3), "peak_at": r["peak_slot_label"],
             "load_factor": round(r["load_factor"], 3)}
            for r in top
        ],
        "load_profile_peaks": [
            {"tariff": r["tariff"], "season": r["season"], "day_type": r["day_type"],
             "households": r["households"], "peak_mean_kw": round(r["p"]["mean_kw"], 3), "at": r["p"]["slot_label"]}
            for r in profile_peak
        ],
        "forecast_metrics": metrics,
    }


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    spark = get_spark()
    lake = Lakehouse(spark, base_path=args.lake) if args.lake else Lakehouse(spark, catalog=args.catalog, schema=args.schema)
    stage = args.stage
    source = args.source or lake.raw_dir()

    if stage == "download":
        lake.ensure_schema(with_raw_volume=True)
        for path in download.download_parts(source, args.parts):
            print(f"downloaded {path}")
        return
    lake.ensure_schema()
    if stage in ("bronze", "all"):
        print(f"bronze rows: {build_bronze(lake, source)}")
    if stage in ("silver", "all"):
        print("data quality:", json.dumps(build_silver(lake, args.max_reject_ratio)))
    if stage in ("gold", "all"):
        print("gold rows:", json.dumps(build_gold(lake)))
    if stage in ("forecast", "all"):
        build_forecast(lake, args.min_households, args.holdout_days)
    if stage in ("views", "all"):
        print("views:", ", ".join(create_views(lake)))
    if stage in ("report", "all"):
        print(json.dumps(report(lake), indent=2))
    sys.stdout.flush()


if __name__ == "__main__":
    main()

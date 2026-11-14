"""End-to-end: real LCL excerpt + injected bad rows -> bronze/silver/gold Delta tables -> SQL views."""

import shutil

import pytest

from gridlens.bronze import BRONZE_TABLE, build_bronze
from gridlens.forecast import BASELINE_NAME, FORECAST_TABLE, MODEL_NAME, build_forecast
from gridlens.gold import DAILY_PEAK_TABLE, HALFHOURLY_TABLE, LOAD_PROFILE_TABLE, build_gold
from gridlens.silver import DataQualityError, build_silver
from gridlens.storage import Lakehouse
from gridlens.views import create_views

from .conftest import FIXTURES

# tests/fixtures/bad_rows.csv: one injected row per rule (two for missing_kwh and
# kwh_out_of_range) plus one exact copy of a real row. The real excerpt itself contains
# 8 genuine duplicate readings (each household repeats 00:00 on 2013-01-21 and 2013-02-21).
EXPECTED_REJECTS = {
    "invalid_household_id": 1,
    "invalid_tariff": 1,
    "unparseable_timestamp": 1,
    "off_interval_timestamp": 1,
    "missing_kwh": 2,
    "non_numeric_kwh": 1,
    "kwh_out_of_range": 2,
    "duplicate_reading": 1 + 8,
}


def count_lines(path) -> int:
    with open(path) as fh:
        return sum(1 for _ in fh) - 1


@pytest.fixture(scope="module")
def lake(spark, tmp_path_factory):
    root = tmp_path_factory.mktemp("lake")
    raw = root / "raw"
    raw.mkdir()
    for name in ("lcl_sample.csv", "bad_rows.csv"):
        shutil.copy(FIXTURES / name, raw / name)
    lake = Lakehouse(spark, base_path=str(root / "lakehouse"))
    lake.ensure_schema()
    lake.raw_glob = str(raw)
    return lake


def test_bronze_lands_every_row(lake):
    n = build_bronze(lake, lake.raw_glob)
    assert n == count_lines(FIXTURES / "lcl_sample.csv") + count_lines(FIXTURES / "bad_rows.csv")
    assert set(lake.read(BRONZE_TABLE).columns) >= {"lcl_id", "kwh_raw", "source_file", "ingested_at"}


def test_silver_rejects_and_reports(lake):
    report = build_silver(lake)
    for rule, n in EXPECTED_REJECTS.items():
        assert report[rule] == n, rule
    rejected = sum(EXPECTED_REJECTS.values())
    assert report["TOTAL_INPUT"] == lake.read(BRONZE_TABLE).count()
    assert report["TOTAL_PASSED"] == report["TOTAL_INPUT"] - rejected
    assert lake.read("silver_rejected").count() == rejected


def test_silver_fails_run_above_reject_threshold(lake):
    with pytest.raises(DataQualityError):
        build_silver(lake, max_reject_ratio=0.0)
    build_silver(lake)  # restore a passing state for the following tests


def test_gold_tables(lake):
    counts = build_gold(lake)
    assert all(counts.values())
    hh = lake.read(HALFHOURLY_TABLE)
    assert hh.where("households < 1 OR total_kwh < 0").count() == 0
    profile = lake.read(LOAD_PROFILE_TABLE)
    # every (tariff, season, day_type) group has all 48 half hours
    per_group = profile.groupBy("tariff", "season", "day_type").count().collect()
    assert per_group and all(r["count"] == 48 for r in per_group)
    peaks = lake.read(DAILY_PEAK_TABLE).where("intervals = 48").collect()
    assert peaks
    for r in peaks:
        assert 0 < r["load_factor"] <= 1
        assert r["peak_kw_per_household"] >= 0


def test_forecast_and_views(lake, spark):
    metrics = build_forecast(lake, min_households=3, holdout_days=7)
    test = {m["model"]: m for m in metrics if m["split"] == "test"}
    assert set(test) == {MODEL_NAME, BASELINE_NAME}
    assert test[MODEL_NAME]["n"] > 0 and test[MODEL_NAME]["mae"] >= 0
    splits = {r["split"]: r["count"] for r in lake.read(FORECAST_TABLE).groupBy("split").count().collect()}
    assert splits["future"] == 7 * 48 and splits["test"] > 0 and splits["train"] > 0

    views = create_views(lake)
    assert views == ["v_load_profile", "v_peak_demand", "v_consumption_forecast", "v_forecast_accuracy",
                     "v_data_quality"]
    for v in views:
        assert spark.sql(f"SELECT * FROM gridlens.{v}").count() > 0, v
    top = spark.sql("SELECT peak_rank_overall FROM gridlens.v_peak_demand ORDER BY peak_rank_overall LIMIT 1")
    assert top.first()[0] == 1

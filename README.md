# GridLens

Smart meter analytics on a Delta Lake medallion architecture. GridLens ingests real
half-hourly household readings from the UK Power Networks Low Carbon London trial into
bronze, silver and gold Delta tables, quarantines and reports bad rows with PySpark
data-quality rules, and builds gold tables for load profiles, peak demand and a 7-day
consumption forecast with a held-out error. The same PySpark code runs locally (Spark 4.0
+ delta-spark in Docker) and as a Databricks job defined in a Databricks Asset Bundle.
Databricks SQL view definitions and a Power BI build sheet sit on top of the gold layer.

## Data

[SmartMeter Energy Consumption Data in London Households](https://data.london.gov.uk/dataset/smartmeter-energy-consumption-data-in-london-households-vqm0d),
UK Power Networks via the London Datastore, CC BY 4.0. 5,567 households, Nov 2011 to
Feb 2014, 167M half-hourly kWh readings, published as a 760 MB zip of 168 one-million-row
CSV files.

`gridlens download` reads the zip's central directory over HTTP range requests and
extracts only the first N member files (default 2, i.e. 2,000,000 rows, about 100 MB
uncompressed), verifying size and CRC32. No data is committed except a real 10,760-row
excerpt (4 households, 2013-01-01 to 2013-02-25) and 10 injected bad rows under
`tests/fixtures/`.

## Architecture

```
London Datastore zip --(HTTP range)--> data/raw/*.csv  |  /Volumes/<catalog>/<schema>/raw
        |
bronze_readings          raw strings + source_file + ingested_at
        |   silver.py: 7 ordered rules + de-duplication
        +--> silver_readings     typed: household_id, tariff, ts (UTC), reading_date, slot, kwh
        +--> silver_rejected     every rejected row with its dq_rule
        +--> dq_report           rows per rule, totals, % of input
        |
gold_halfhourly_demand   per half hour: households reporting, total kWh, mean kWh/kW per household
gold_load_profile        tariff x season x weekday/weekend x half hour: mean, p50, p90
gold_daily_peak          per day: kWh/household, peak kW/household and when, aggregate peak, load factor
gold_forecast            actual / model / baseline per half hour, split = train | test | future
gold_forecast_metrics    MAE, RMSE, WAPE per model and split
        |
SQL views (src/gridlens/sql/views.sql): v_load_profile, v_peak_demand,
v_consumption_forecast, v_forecast_accuracy, v_data_quality  -->  Power BI (docs/powerbi.md)
```

Storage is abstracted by `Lakehouse` (`src/gridlens/storage.py`): locally each table is a
Delta directory under `lakehouse/` and the views are created over tables registered in
the local catalog; on Databricks the same writes go to Unity Catalog managed tables
`<catalog>.<schema>.<table>` and the raw files to a UC volume.

**Data-quality rules** (`src/gridlens/silver.py`), evaluated in order; a row is rejected
with the first rule it fails:

| rule | fails when |
|---|---|
| invalid_household_id | id missing or not `MAC` + 6 digits |
| invalid_tariff | tariff group not `Std` or `ToU` |
| unparseable_timestamp | DateTime missing or not `yyyy-MM-dd HH:mm:ss` |
| off_interval_timestamp | timestamp not on :00 or :30 |
| missing_kwh | reading empty or the literal `Null` |
| non_numeric_kwh | reading not a number |
| kwh_out_of_range | below 0 or above 11.5 kWh per half hour (100 A x 230 V x 0.5 h) |
| duplicate_reading | second or later valid reading for the same household and half hour |

The silver step fails the run (`DataQualityError`) if more than 5% of rows are rejected
(`--max-reject-ratio`). Parsing uses `try_to_timestamp` / `try_cast` so malformed values
become rejections instead of ANSI-mode exceptions.

**Forecast** (`src/gridlens/forecast.py`). Target: mean kWh per reporting household per
half hour, over half hours with at least 20 households reporting (households join and
leave the trial). Model: ordinary least squares on `y(t-7d)`, `y(t-14d)`, half-hour-of-day
and day-of-week dummies. All features are known a week ahead, so one fit forecasts the
next 7 days directly. Baseline: seasonal naive `y(t-7d)`. The last 28 days are held out;
the model is fitted on the earlier data only and both model and baseline are scored on the
holdout, then the model is refitted on all history to produce the 7 future days. The
series has one row per half hour, so it is fitted with numpy on the driver.

## Quickstart (local, Docker)

Java is not required on the host; the image bundles Python 3.12, a Temurin 21 JRE,
pyspark 4.0.1 and delta-spark 4.0.0. The compose service is capped at 2 GB RAM / 2 CPUs
with a 1 GB driver.

```bash
docker compose build
PYTHONPATH=src python3 -m gridlens.download --out data/raw --parts 2   # stdlib only, no Spark needed
docker compose run --rm spark gridlens all --lake lakehouse --source data/raw
docker compose run --rm spark pytest -q
docker compose down -v
```

`GRIDLENS_LAKE_DIR=/some/fast/disk` relocates the Delta output. Without Docker:
`uv sync --extra local --group dev` (needs Java 17/21) then
`uv run gridlens all --lake lakehouse --source data/raw`.

## Databricks

`databricks.yml` defines the bundle: a wheel artifact built with `uv build`, and a
serverless job `gridlens-pipeline` (environment version 4) with tasks
`download_raw -> bronze -> silver_quality -> gold -> forecast -> sql_views -> report`.
The first six are `python_wheel_task`s calling the same `gridlens` CLI with
`--catalog/--schema`; `report` runs `notebooks/gridlens_report.py`, which displays the
views and fails if any gold output is empty. Variables: `catalog` (default `workspace`, the
Free Edition catalog), `schema`, `parts`, `min_households`, `holdout_days`.

```bash
databricks auth login --host https://<workspace>.cloud.databricks.com
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev gridlens_pipeline
```

## Power BI

`docs/powerbi.md` specifies the Databricks SQL connection, the five views to import, a
date table and relationships, the DAX measures and a four-page layout (load profiles,
peak demand, forecast, data quality).

## Tests

`pytest` (14 tests): each DQ rule on crafted rows, including that an invalid row cannot
make a valid row a duplicate; an end-to-end run on the real fixture plus injected bad rows
that checks exact per-rule reject counts, the reject-ratio failure, gold invariants (48
slots per profile group, 0 < load factor <= 1), the forecast splits and future horizon,
and that every SQL view resolves; the forecast on synthetic series (exact recovery of a
weekly pattern, gaps, too-short history); and the range-request zip extractor against a
local HTTP server. CI (GitHub Actions) runs the suite on Java 21 + Python 3.12 and builds
the wheel.

## Measured results

All numbers below come from `docker compose run --rm spark gridlens all --lake lakehouse
--source data/raw` on the first 2 source files, followed by queries on the resulting gold
tables. The run printed the report as JSON; it took 101 s wall-clock in the 2 GB / 2 CPU
container on a shared, heavily loaded 8-core machine.

**Ingestion and data quality**

| | rows |
|---|---|
| bronze (input) | 2,000,000 |
| silver (passed) | 1,998,565 |
| rejected: off_interval_timestamp | 56 |
| rejected: duplicate_reading | 1,379 |
| rejected: all other rules | 0 |

Rejected share: 0.072%. All 56 off-interval rows (timestamps such as `hh:13:41`) also
carry the literal `Null` as their reading; they are counted under the first rule they
fail. Silver covers 56 households from 2011-12-06 13:00 to 2014-02-28 00:00 UTC, all on
the standard tariff.

**Gold tables:** 39,095 half hours in `gold_halfhourly_demand`, 384 load-profile rows
(1 tariff x 4 seasons x 2 day types x 48 slots), 816 days in `gold_daily_peak`, of which
814 have all 48 intervals.

**Load profiles** (mean kW per household, highest half hour of each profile):

| season | weekday | weekend |
|---|---|---|
| winter | 0.790 at 19:00 | 0.812 at 19:00 |
| autumn | 0.676 at 19:00 | 0.676 at 19:00 |
| spring | 0.643 at 20:00 | 0.648 at 20:00 |
| summer | 0.509 at 21:00 | 0.481 at 21:00 |

(Autumn weekday and weekend round to the same value: 0.6756 vs 0.6765.)

**Peak demand** over the 814 complete days: mean 11.48 kWh per household per day, mean
daily peak 0.744 kW per household, mean load factor 0.646. The most common peak half
hours are 19:00 (100 days), 18:30 (93) and 00:00 (83). Highest daily peaks:

| date | households | peak kW / household | at | load factor |
|---|---|---|---|---|
| 2012-02-12 | 46 | 1.271 | 19:30 | 0.578 |
| 2012-02-02 | 46 | 1.162 | 18:30 | 0.602 |
| 2012-02-03 | 46 | 1.128 | 18:30 | 0.608 |
| 2012-01-14 | 45 | 1.108 | 17:30 | 0.576 |
| 2012-01-17 | 45 | 1.100 | 18:30 | 0.561 |

**Forecast** (mean kWh per household per half hour; holdout = 1,344 half hours from
2014-01-31 00:30 to 2014-02-28 00:00; training = 37,025 half hours from 2011-12-21 16:00):

| model | split | MAE (kWh) | RMSE (kWh) | WAPE |
|---|---|---|---|---|
| ols_weekly_lags | test | 0.0276 | 0.0354 | 10.41% |
| seasonal_naive_7d | test | 0.0334 | 0.0430 | 12.62% |
| ols_weekly_lags | train | 0.0298 | 0.0393 | 12.57% |
| seasonal_naive_7d | train | 0.0332 | 0.0443 | 13.99% |

On the holdout the regression cuts MAE by 17% relative to the seasonal-naive baseline.
The refitted model forecasts 336 half hours (2014-02-28 00:30 to 2014-03-07 00:00),
85.37 kWh per household in total.

Delta versioning works as expected: re-running the pipeline over the same directory
produces version 1 (`WRITE`, overwrite) on top of version 0 in `DESCRIBE HISTORY`.

## Limitations

- **Not run on Databricks.** No Databricks workspace was available. The bundle was checked
  offline against the JSON schema emitted by Databricks CLI v1.26.0
  (`databricks bundle schema` + `check-jsonschema`: valid), but `databricks bundle validate`
  itself requires workspace credentials and was **not** run; nothing has been deployed.
  Serverless-specific behaviour (UC volume writes from the download task, wheel install in
  environment version 4) is untested.
- **No Power BI report.** Power BI Desktop is Windows-only and was not available; there is
  no `.pbix`. `docs/powerbi.md` is a specification, not a built report.
- **No Databricks SQL warehouse.** The views were executed with local Spark SQL, not on a
  Databricks SQL warehouse.
- **Sample, not the full dataset.** Measured numbers come from the first 2 of 168 source
  files (2,000,000 rows, 56 households, all on the standard tariff, so the ToU split of the
  load profile is empty in this sample). Use `--parts` to ingest more.
- The forecast is deliberately simple: no weather or holiday features. Lags at 7 and 14
  days restrict the direct horizon to 7 days.

## License

MIT for the code. The data is CC BY 4.0, UK Power Networks.

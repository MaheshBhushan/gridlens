-- GridLens reporting views over the gold layer.
-- {schema} is replaced by the target schema: `catalog`.`schema` on Databricks (Unity
-- Catalog), `gridlens` locally. Spark SQL / Databricks SQL dialect. Power BI connects to
-- these views through a Databricks SQL warehouse (see docs/powerbi.md).

-- Average half-hourly load shape by tariff group, season and weekday/weekend.
CREATE OR REPLACE VIEW {schema}.v_load_profile AS
SELECT
  tariff,
  season,
  day_type,
  slot,
  slot_label,
  households,
  readings,
  mean_kwh,
  p50_kwh,
  p90_kwh,
  mean_kw AS mean_kw_per_household
FROM {schema}.gold_load_profile;

-- Daily peak demand with ranks overall and within each calendar month.
CREATE OR REPLACE VIEW {schema}.v_peak_demand AS
SELECT
  reading_date,
  date_trunc('MONTH', reading_date) AS month_start,
  households,
  intervals,
  kwh_per_household,
  peak_kw_per_household,
  peak_ts,
  peak_slot_label,
  peak_aggregate_kw,
  load_factor,
  RANK() OVER (ORDER BY peak_kw_per_household DESC) AS peak_rank_overall,
  RANK() OVER (PARTITION BY date_trunc('MONTH', reading_date) ORDER BY peak_kw_per_household DESC)
    AS peak_rank_in_month
FROM {schema}.gold_daily_peak
WHERE intervals = 48;

-- Actuals, model forecast and seasonal-naive baseline per half hour, incl. 7 future days.
CREATE OR REPLACE VIEW {schema}.v_consumption_forecast AS
SELECT
  ts,
  CAST(ts AS DATE) AS forecast_date,
  slot,
  split,
  model,
  actual_kwh,
  predicted_kwh,
  baseline_kwh,
  predicted_kwh * 2 AS predicted_kw_per_household,
  ABS(predicted_kwh - actual_kwh) AS abs_error_kwh
FROM {schema}.gold_forecast;

-- Held-out and in-sample error for the model and the baseline.
CREATE OR REPLACE VIEW {schema}.v_forecast_accuracy AS
SELECT model, split, n, mae, rmse, wape_pct, period_start, period_end, min_households
FROM {schema}.gold_forecast_metrics;

-- Data-quality outcome of the latest silver run.
CREATE OR REPLACE VIEW {schema}.v_data_quality AS
SELECT rule, description, rows, pct_of_input, checked_at
FROM {schema}.dq_report;

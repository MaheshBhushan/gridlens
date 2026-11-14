"""Gold forecast: 7-day-ahead half-hourly consumption per household, with held-out evaluation.

Target: ``mean_kwh`` from ``gold_halfhourly_demand`` (mean kWh per reporting household in
a half hour), restricted to half hours with at least ``min_households`` reporting.

Model: ordinary least squares on features that are all known 7 days in advance, so a
single fit forecasts the next 7 days directly without recursion:

    y(t) ~ 1 + y(t - 7d) + y(t - 14d) + half-hour-of-day dummies + day-of-week dummies

Baseline: seasonal naive, y_hat(t) = y(t - 7d).

Evaluation: the last ``holdout_days`` of the series are held out. The model is fitted on
the earlier part only; both model and baseline are scored on the holdout (MAE, RMSE and
WAPE = sum|error| / sum(actual)). The model is then refitted on all history and used to
forecast the 7 days after the last observation.

The series has one row per half hour (tens of thousands of rows at most), so it is
collected to the driver and fitted with numpy; this works the same on a laptop and on
Databricks serverless compute.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from gridlens.gold import HALFHOURLY_TABLE
from gridlens.storage import Lakehouse

FORECAST_TABLE = "gold_forecast"
METRICS_TABLE = "gold_forecast_metrics"
SLOTS_PER_DAY = 48
LAG_DAYS = (7, 14)
HORIZON_DAYS = 7
MODEL_NAME = "ols_weekly_lags"
BASELINE_NAME = "seasonal_naive_7d"

FORECAST_SCHEMA = (
    "ts long, slot int, split string, actual_kwh double, predicted_kwh double, baseline_kwh double, model string"
)
METRICS_SCHEMA = (
    "model string, split string, n long, mae double, rmse double, wape_pct double, "
    "period_start long, period_end long, min_households int"
)


def load_series(halfhourly: DataFrame, min_households: int) -> pd.Series:
    rows = (
        halfhourly.where(F.col("households") >= min_households)
        .select(F.unix_timestamp("ts").alias("epoch"), "mean_kwh")
        .orderBy("epoch")
        .collect()
    )
    if not rows:
        raise ValueError(f"no half hours with >= {min_households} households")
    index = pd.to_datetime([r["epoch"] for r in rows], unit="s")
    s = pd.Series([r["mean_kwh"] for r in rows], index=index, dtype=float)
    full = pd.date_range(s.index.min(), s.index.max(), freq="30min")
    return s.reindex(full)


def design(index: pd.DatetimeIndex, lags: dict[int, np.ndarray]) -> np.ndarray:
    slot = (index.hour * 2 + index.minute // 30).to_numpy()
    dow = index.day_of_week.to_numpy()
    cols = [np.ones(len(index))]
    cols += [lags[d] for d in LAG_DAYS]
    cols += [(slot == k).astype(float) for k in range(1, SLOTS_PER_DAY)]
    cols += [(dow == k).astype(float) for k in range(1, 7)]
    return np.column_stack(cols)


def lag_values(series: pd.Series, index: pd.DatetimeIndex) -> dict[int, np.ndarray]:
    return {d: series.reindex(index - pd.Timedelta(days=d)).to_numpy() for d in LAG_DAYS}


def fit(x: np.ndarray, y: np.ndarray) -> np.ndarray:
    coef, *_ = np.linalg.lstsq(x, y, rcond=None)
    return coef


def score(y: np.ndarray, y_hat: np.ndarray) -> dict[str, float]:
    err = y_hat - y
    return {
        "n": int(len(y)),
        "mae": float(np.mean(np.abs(err))),
        "rmse": float(np.sqrt(np.mean(err**2))),
        "wape_pct": float(np.sum(np.abs(err)) / np.sum(np.abs(y)) * 100),
    }


def run_forecast(series: pd.Series, holdout_days: int) -> tuple[pd.DataFrame, list[dict]]:
    """Return (forecast rows, metric rows) for a regular half-hourly series."""
    index = series.index
    lags = lag_values(series, index)
    usable = series.notna().to_numpy() & np.all([~np.isnan(lags[d]) for d in LAG_DAYS], axis=0)
    cutoff = index.max() - pd.Timedelta(days=holdout_days)
    is_test = (index > cutoff) & usable
    is_train = (index <= cutoff) & usable
    if is_train.sum() < 2 * SLOTS_PER_DAY * 7 or is_test.sum() == 0:
        raise ValueError("not enough history: need >= 14 days of lag history plus train and test periods")

    x = design(index, lags)
    y = series.to_numpy()
    coef = fit(x[is_train], y[is_train])
    pred = x @ coef
    baseline = lags[7]

    metrics = []
    for split, mask in (("train", is_train), ("test", is_test)):
        period = index[mask]
        for model, y_hat in ((MODEL_NAME, pred), (BASELINE_NAME, baseline)):
            metrics.append(
                {"model": model, "split": split, **score(y[mask], y_hat[mask]),
                 "period_start": period.min(), "period_end": period.max()}
            )

    history = pd.DataFrame(
        {"ts": index, "split": np.where(is_test, "test", "train"), "actual_kwh": y,
         "predicted_kwh": pred, "baseline_kwh": baseline}
    )[usable]

    # Refit on all usable history, then forecast the next HORIZON_DAYS directly.
    usable_all = is_train | is_test
    coef_all = fit(x[usable_all], y[usable_all])
    future_index = pd.date_range(index.max() + pd.Timedelta(minutes=30), periods=HORIZON_DAYS * SLOTS_PER_DAY,
                                 freq="30min")
    future_lags = lag_values(series, future_index)
    future_ok = np.all([~np.isnan(future_lags[d]) for d in LAG_DAYS], axis=0)
    future = pd.DataFrame(
        {"ts": future_index, "split": "future", "actual_kwh": np.nan,
         "predicted_kwh": design(future_index, future_lags) @ coef_all, "baseline_kwh": future_lags[7]}
    )[future_ok]

    out = pd.concat([history, future], ignore_index=True)
    out["slot"] = (out["ts"].dt.hour * 2 + out["ts"].dt.minute // 30).astype(int)
    return out, metrics


def _epoch(value: pd.Timestamp) -> int:
    # Timestamps cross the Python/Spark boundary as epoch seconds (UTC) so neither the
    # driver's local time zone nor the session time zone can shift them.
    return int(value.value // 1_000_000_000)


def _with_timestamps(df: DataFrame, *cols: str) -> DataFrame:
    for c in cols:
        df = df.withColumn(c, F.timestamp_seconds(F.col(c)))
    return df


def build_forecast(lake: Lakehouse, min_households: int, holdout_days: int = 28) -> list[dict]:
    series = load_series(lake.read(HALFHOURLY_TABLE), min_households)
    out, metrics = run_forecast(series, holdout_days)
    spark = lake.spark

    def nan_to_none(v: float) -> float | None:
        return None if v is None or np.isnan(v) else float(v)

    forecast_rows = [
        (_epoch(r.ts), int(r.slot), r.split, nan_to_none(r.actual_kwh), nan_to_none(r.predicted_kwh),
         nan_to_none(r.baseline_kwh), MODEL_NAME)
        for r in out.itertuples(index=False)
    ]
    lake.write(_with_timestamps(spark.createDataFrame(forecast_rows, FORECAST_SCHEMA), "ts"), FORECAST_TABLE)
    metric_rows = [
        (m["model"], m["split"], m["n"], m["mae"], m["rmse"], m["wape_pct"], _epoch(m["period_start"]),
         _epoch(m["period_end"]), min_households)
        for m in metrics
    ]
    metrics_df = _with_timestamps(spark.createDataFrame(metric_rows, METRICS_SCHEMA), "period_start", "period_end")
    lake.write(metrics_df, METRICS_TABLE)
    return metrics

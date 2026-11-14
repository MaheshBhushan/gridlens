import numpy as np
import pandas as pd
import pytest

from gridlens.forecast import BASELINE_NAME, HORIZON_DAYS, MODEL_NAME, SLOTS_PER_DAY, run_forecast, score


def weekly_series(days: int, noise: float = 0.0, seed: int = 0) -> pd.Series:
    idx = pd.date_range("2013-01-07", periods=days * SLOTS_PER_DAY, freq="30min")
    slot = idx.hour * 2 + idx.minute // 30
    base = 0.15 + 0.1 * np.sin(2 * np.pi * slot / SLOTS_PER_DAY) + 0.05 * (idx.day_of_week >= 5)
    rng = np.random.default_rng(seed)
    return pd.Series(base + noise * rng.standard_normal(len(idx)), index=idx)


def test_score():
    m = score(np.array([1.0, 2.0, 3.0]), np.array([1.0, 3.0, 1.0]))
    assert m["n"] == 3
    assert m["mae"] == pytest.approx(1.0)
    assert m["rmse"] == pytest.approx(np.sqrt(5 / 3))
    assert m["wape_pct"] == pytest.approx(50.0)


def test_exact_weekly_pattern_is_recovered():
    out, metrics = run_forecast(weekly_series(42), holdout_days=7)
    test = {m["model"]: m for m in metrics if m["split"] == "test"}
    assert test[MODEL_NAME]["mae"] < 1e-9
    assert test[BASELINE_NAME]["mae"] < 1e-9
    assert test[MODEL_NAME]["n"] == 7 * SLOTS_PER_DAY


def test_splits_and_future_horizon():
    series = weekly_series(35, noise=0.02)
    out, metrics = run_forecast(series, holdout_days=7)
    counts = out["split"].value_counts().to_dict()
    assert counts["test"] == 7 * SLOTS_PER_DAY
    # The first 14 days have no lag-14 value, so they are not usable rows.
    assert counts["train"] == (35 - 14 - 7) * SLOTS_PER_DAY
    assert counts["future"] == HORIZON_DAYS * SLOTS_PER_DAY
    future = out[out["split"] == "future"]
    assert future["ts"].min() == series.index.max() + pd.Timedelta(minutes=30)
    assert future["actual_kwh"].isna().all() and future["predicted_kwh"].notna().all()
    assert set(out["slot"]) == set(range(SLOTS_PER_DAY))
    test = {m["model"]: m for m in metrics if m["split"] == "test"}
    # Averaging two weekly lags halves the noise variance, so the model must beat the naive lag.
    assert test[MODEL_NAME]["rmse"] < test[BASELINE_NAME]["rmse"]


def test_gaps_are_skipped():
    series = weekly_series(42)
    series.iloc[20 * SLOTS_PER_DAY : 21 * SLOTS_PER_DAY] = np.nan
    out, _ = run_forecast(series, holdout_days=7)
    assert out.loc[out["split"] != "future", "actual_kwh"].notna().all()


def test_too_short_history_raises():
    with pytest.raises(ValueError):
        run_forecast(weekly_series(16), holdout_days=7)

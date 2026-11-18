from datetime import date

import pandas as pd
import pytest

from jtf.ml.backtest import (
    PERSISTENCE,
    PREDICTION_COLUMNS,
    fold_months,
    last_complete_month,
    metrics,
    month,
    persistence,
)


def test_the_last_fold_is_the_newest_month_with_every_day_labelled():
    assert last_complete_month(date(2026, 11, 16)) == date(2026, 10, 1)
    assert last_complete_month(date(2026, 10, 31)) == date(2026, 10, 1)
    assert last_complete_month(date(2026, 10, 30)) == date(2026, 9, 1)
    assert last_complete_month(date(2024, 12, 31)) == date(2024, 12, 1)
    with pytest.raises(ValueError, match="ads_ml_label_solar"):
        last_complete_month(None)


def test_folds_are_every_month_from_the_first_to_the_last():
    folds = fold_months(month("2025-03"), month("2026-10"))
    assert len(folds) == 20
    assert folds[:2] == [date(2025, 3, 1), date(2025, 4, 1)]
    assert folds[-1] == date(2026, 10, 1)
    with pytest.raises(ValueError):
        fold_months(month("2026-10"), month("2025-03"))
    with pytest.raises(ValueError):
        fold_months(date(2025, 3, 15), month("2025-06"))


def test_persistence_predicts_each_slot_with_the_same_slot_two_days_before():
    features = pd.DataFrame(
        {
            "fold": [date(2025, 3, 1)] * 2,
            "decision_time": pd.to_datetime(["2025-03-01 00:00:00+00:00"] * 2),
            "target_day": [date(2025, 3, 2)] * 2,
            "slot_start": pd.to_datetime(["2025-03-02 03:00+00:00", "2025-03-02 03:30+00:00"]),
            "solar_d2_mw": [5000.0, 5100.0],
        }
    )
    rows = persistence(features, run_id="r1")
    assert list(rows.columns) == PREDICTION_COLUMNS
    assert rows["predicted_mw"].tolist() == [5000.0, 5100.0]
    assert set(rows["run_id"]) == {"r1"}
    assert set(rows["model"]) == {PERSISTENCE}
    assert set(rows["mode"]) == {"backtest"}


def test_metrics_score_labelled_daylight_slots_as_shares_of_capacity():
    # Capacity 20,000 MW. March: +100 and −300 MW in daylight, and a night slot that must not
    # count. April: +600 MW, and a daylight slot with no label yet that must not count either.
    outcomes = pd.DataFrame(
        {
            "fold": [date(2025, 3, 1)] * 3 + [date(2025, 4, 1)] * 2,
            "error_mw": [100.0, -300.0, 50.0, 600.0, None],
            "error_share": [0.005, -0.015, 0.0025, 0.03, None],
            "is_daylight": [True, True, False, True, True],
        }
    )
    per_fold, overall = metrics(outcomes)
    assert per_fold["fold"].tolist() == [date(2025, 3, 1), date(2025, 4, 1)]
    assert per_fold["slots"].tolist() == [2, 1]
    assert per_fold["mae_share"].tolist() == pytest.approx([0.01, 0.03])
    assert per_fold["bias_share"].tolist() == pytest.approx([-0.005, 0.03])
    assert per_fold["mae_mw"].tolist() == pytest.approx([200.0, 600.0])
    # Overall weighs every slot alike, not every fold: (0.005 + 0.015 + 0.03) / 3, not 0.02.
    assert overall == pytest.approx(
        {"slots": 3, "mae_share": 0.05 / 3, "bias_share": 0.02 / 3, "mae_mw": 1000 / 3}
    )

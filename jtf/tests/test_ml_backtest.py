from datetime import date

import pandas as pd
import pytest

from jtf.ml.backtest import (
    NOT_YET,
    PERSISTENCE,
    PREDICTION_COLUMNS,
    benchmark,
    by_group,
    fold_months,
    last_complete_month,
    metrics,
    month,
    persistence,
)
from jtf.ml.tracking import group_metrics


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


def test_the_same_metrics_come_per_kind_of_day_and_ungrouped_days_are_named():
    # Capacity 20,000 MW. A clear, curtailed spring day: +100 and −300 MW. A cloudy, normal
    # spring day: +600 MW. A day not grouped yet: +200 MW, counted under its own name. A night
    # slot and a slot with no label count nowhere.
    days = [date(2025, 3, 1)] * 3 + [date(2025, 3, 2), date(2025, 3, 30), date(2025, 3, 30)]
    outcomes = pd.DataFrame(
        {
            "fold": [date(2025, 3, 1)] * 6,
            "target_day": days,
            "error_mw": [100.0, -300.0, 50.0, 600.0, 200.0, None],
            "error_share": [0.005, -0.015, 0.0025, 0.03, 0.01, None],
            "is_daylight": [True, True, False, True, True, True],
            "sky": ["clear"] * 3 + ["cloudy", None, None],
            "is_curtailed_day": [True] * 3 + [False] * 3,
            "season": ["spring"] * 6,
        }
    )
    groups = by_group(outcomes).set_index(["grouping", "group"])

    assert set(groups.index) == {
        ("sky", "clear"), ("sky", "cloudy"), ("sky", NOT_YET),
        ("curtailed", "curtailed"), ("curtailed", "normal"), ("season", "spring"),
    }  # fmt: skip
    assert groups["slots"].to_dict() == {
        ("sky", "clear"): 2, ("sky", "cloudy"): 1, ("sky", NOT_YET): 1,
        ("curtailed", "curtailed"): 2, ("curtailed", "normal"): 2, ("season", "spring"): 4,
    }  # fmt: skip
    assert groups.loc[("sky", "clear")].to_dict() == pytest.approx(
        {"days": 1, "slots": 2, "mae_share": 0.01, "bias_share": -0.005, "mae_mw": 200.0}
    )
    assert groups.loc[("curtailed", "normal"), "days"] == 2
    assert groups.loc[("curtailed", "normal"), "mae_mw"] == pytest.approx(400.0)
    # Every grouping covers every scored slot once
    assert groups["slots"].groupby("grouping").sum().to_dict() == {
        "sky": 4, "curtailed": 4, "season": 4
    }  # fmt: skip


def test_metrics_per_kind_of_day_reach_mlflow_under_names_that_say_which():
    groups = pd.DataFrame(
        {
            "grouping": ["sky", "sky"],
            "group": ["clear", NOT_YET],
            "days": [3, 1],
            "slots": [80, 20],
            "mae_share": [0.08, 0.11],
            "bias_share": [0.01, -0.02],
            "mae_mw": [1600.0, 2200.0],
        }
    )
    assert group_metrics(groups) == {
        "sky/clear/mae_share": 0.08, "sky/clear/bias_share": 0.01, "sky/clear/mae_mw": 1600.0,
        f"sky/{NOT_YET}/mae_share": 0.11, f"sky/{NOT_YET}/bias_share": -0.02,
        f"sky/{NOT_YET}/mae_mw": 2200.0,
    }  # fmt: skip


def test_a_run_with_a_range_is_also_scored_by_its_coverage_and_pinball_loss():
    # Four daylight slots: the label inside the range in three. A night slot and a slot with no
    # label count nowhere. A run without a range has neither score.
    outcomes = pd.DataFrame(
        {
            "fold": [date(2025, 3, 1)] * 6,
            "target_day": [date(2025, 3, 1)] * 6,
            "error_mw": [100.0, -300.0, 50.0, 600.0, 200.0, None],
            "error_share": [0.005, -0.015, 0.0025, 0.03, 0.01, None],
            "is_daylight": [True, True, False, True, True, True],
            "in_range": [True, True, True, False, True, None],
            "pinball_share": [0.002, 0.004, 0.001, 0.02, 0.006, None],
            "sky": ["clear"] * 6,
            "is_curtailed_day": [False] * 6,
            "season": ["spring"] * 6,
        }
    )
    per_fold, overall = metrics(outcomes)
    assert overall["coverage"] == pytest.approx(0.75)
    assert overall["pinball_share"] == pytest.approx(0.008)
    assert per_fold["coverage"].tolist() == pytest.approx([0.75])
    assert (
        by_group(outcomes).set_index(["grouping", "group"]).loc[("sky", "clear"), "coverage"]
        == 0.75
    )
    assert group_metrics(by_group(outcomes))["sky/clear/pinball_share"] == pytest.approx(0.008)

    no_range = outcomes.drop(columns=["in_range", "pinball_share"])
    assert set(metrics(no_range)[1]) == {"slots", "mae_share", "bias_share", "mae_mw"}
    assert "coverage" not in by_group(no_range) and "coverage" not in metrics(no_range)[0]


def test_the_price_weighted_error_weighs_each_miss_by_what_the_slot_was_worth():
    # March: misses of 0.01 at 10 yen and 0.03 at 30 yen: (0.1 + 0.9) / 40 = 0.025, where the
    # plain error is 0.02. April: a miss of 0.02 at 20 yen, and one of 0.5 in a slot with no
    # price, which the weighted error leaves out and the plain one doesn't. A night slot counts
    # nowhere. Its price would have moved March's figure.
    outcomes = pd.DataFrame(
        {
            "fold": [date(2025, 3, 1)] * 3 + [date(2025, 4, 1)] * 2,
            "target_day": [date(2025, 3, 1)] * 3 + [date(2025, 4, 1)] * 2,
            "error_mw": [200.0, -600.0, 50.0, 400.0, 10000.0],
            "error_share": [0.01, -0.03, 0.0025, 0.02, 0.5],
            "is_daylight": [True, True, False, True, True],
            "price_yen_kwh": [10.0, 30.0, 999.0, 20.0, None],
            "sky": ["clear"] * 3 + ["cloudy"] * 2,
            "is_curtailed_day": [False] * 5,
            "season": ["spring"] * 5,
        }
    )
    per_fold, overall = metrics(outcomes)

    assert per_fold["mae_share"].tolist() == pytest.approx([0.02, 0.26])
    assert per_fold["mae_share_priced"].tolist() == pytest.approx([0.025, 0.02])
    assert overall["mae_share_priced"] == pytest.approx((0.1 + 0.9 + 0.4) / 60)
    assert {"priced_miss", "price"}.isdisjoint(per_fold.columns) and "price" not in overall
    groups = by_group(outcomes).set_index(["grouping", "group"])
    assert groups.loc[("sky", "clear"), "mae_share_priced"] == pytest.approx(0.025)
    assert group_metrics(by_group(outcomes))["sky/cloudy/mae_share_priced"] == pytest.approx(0.02)

    unpriced = outcomes.assign(price_yen_kwh=None)
    assert "mae_share_priced" not in metrics(unpriced)[1]
    assert "mae_share_priced" not in metrics(outcomes.drop(columns="price_yen_kwh"))[0]


def test_a_run_beside_tepcos_forecast_is_scored_where_both_have_a_miss():
    # Capacity 20,000 MW. The 16th: the run misses by 0.03 and 0.01, TEPCO by 0.01 and 0.01. The
    # 17th: one slot, 0.05 against 0.02. Left out: a night slot; a slot of the 18th with no
    # actuals yet; and a slot of the 18th where only TEPCO has a miss, the run's wind two days
    # back being unknown. So the 18th is no day of the comparison.
    days = [date(2026, 11, 16)] * 3 + [date(2026, 11, 17)] + [date(2026, 11, 18)] * 2
    rows = pd.DataFrame(
        {
            "target_day": days,
            "error_mw": [-600.0, 200.0, 5000.0, 1000.0, None, None],
            "error_share": [-0.03, 0.01, 0.25, 0.05, None, None],
            "tepco_error_mw": [200.0, -200.0, 5000.0, -400.0, None, 1800.0],
            "tepco_error_share": [0.01, -0.01, 0.25, -0.02, None, 0.09],
            "is_daylight": [True, True, False, True, True, True],
        }
    )
    per_day, overall = benchmark(rows)

    assert per_day["target_day"].tolist() == [date(2026, 11, 16), date(2026, 11, 17)]
    assert per_day["slots"].tolist() == [2, 1]
    assert per_day["mae_share"].tolist() == pytest.approx([0.02, 0.05])
    assert per_day["tepco_mae_share"].tolist() == pytest.approx([0.01, 0.02])
    assert overall == pytest.approx(
        {
            "days": 2,
            "slots": 3,
            "mae_share": 0.03,
            "tepco_mae_share": 0.04 / 3,
            "bias_share": 0.01,
            "tepco_bias_share": -0.02 / 3,
            "mae_mw": 600.0,
            "tepco_mae_mw": 800 / 3,
        }
    )


def test_a_run_that_shares_no_day_with_tepcos_capture_has_no_comparison():
    rows = pd.DataFrame(
        {
            "target_day": [date(2026, 11, 18)],
            "error_mw": [None],
            "error_share": [None],
            "tepco_error_mw": [None],
            "tepco_error_share": [None],
            "is_daylight": [True],
        }
    )
    per_day, overall = benchmark(rows)

    assert per_day.empty and overall == {"days": 0, "slots": 0}
    assert benchmark(rows.iloc[:0])[1] == {"days": 0, "slots": 0}

"""The rolling backtest's parts that need no warehouse: its folds, the persistence baseline and the
metrics. Reading and writing BigQuery is in warehouse.py; logging to MLflow in tracking.py.

A fold is a calendar month of target days. The first test month comes after 12 months of history
from the first target day, 2024-02-17 (plan, Model and evaluation); the last is the newest month
whose every slot has a label, so a run's folds never change once scored.
"""

from datetime import date, timedelta

import pandas as pd

PERSISTENCE = "persistence_d2"
MEAN_7D = "mean_7d"
FIRST_TEST_MONTH = date(2025, 3, 1)
BACKTEST = "backtest"

PREDICTION_COLUMNS = [
    "run_id",
    "model",
    "mode",
    "fold",
    "decision_time",
    "target_day",
    "slot_start",
    "predicted_mw",
]


def month(text: str) -> date:
    """`2025-03` as the month's first day."""
    return date.fromisoformat(f"{text}-01")


def last_complete_month(newest_complete_day: date | None) -> date:
    """The newest month with every day labelled, given the newest day with all 48 slots labelled.
    Labels run without gaps, so that is the month before the day after it."""
    if newest_complete_day is None:
        raise ValueError("no day has all 48 slots labelled: build ads_ml_label_solar first")
    following = newest_complete_day + timedelta(days=1)
    return (pd.Timestamp(following.year, following.month, 1) - pd.DateOffset(months=1)).date()


def fold_months(first: date, last: date) -> list[date]:
    """Every month from `first` to `last`, both included, as their first days."""
    months = [m.date() for m in pd.date_range(first, last, freq="MS")]
    if not months or months[0] != first:
        raise ValueError(f"no test month from {first} to {last}")
    return months


def persistence(features: pd.DataFrame, run_id: str) -> pd.DataFrame:
    """The same slot on D−2, the newest day complete at T: the baseline every model must beat.
    `features` holds `dws_solar_features` rows with their fold."""
    return features.assign(
        run_id=run_id, model=PERSISTENCE, mode=BACKTEST, predicted_mw=features["solar_d2_mw"]
    )[PREDICTION_COLUMNS]


AGGREGATES = {
    "slots": ("error_mw", "size"),
    "mae_share": ("abs_share", "mean"),
    "bias_share": ("error_share", "mean"),
    "mae_mw": ("abs_mw", "mean"),
}


def metrics(outcomes: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, float]]:
    """Per fold and overall, over daylight slots with a label (plan, Metrics): MAE and bias as a
    share of the capacity at each decision time (`error_share` from ads_ml_outcome_solar), and
    MAE in MW. `outcomes` must come sorted, so the means add the same floats in the same order
    every run.
    """
    scored = outcomes[outcomes["is_daylight"].eq(True) & outcomes["error_mw"].notna()]
    scored = scored.assign(abs_share=scored["error_share"].abs(), abs_mw=scored["error_mw"].abs())
    per_fold = scored.groupby("fold").agg(**AGGREGATES).reset_index()
    overall = scored.groupby(lambda _: "overall").agg(**AGGREGATES).iloc[0]
    return per_fold, {name: float(value) for name, value in overall.items()}


NOT_YET = "not yet grouped"
CURTAILED = {True: "curtailed", False: "normal"}


def by_group(outcomes: pd.DataFrame) -> pd.DataFrame:
    """The same metrics per kind of day (plan, Segments): by the target day's sky (clear, mixed,
    cloudy), by whether it was curtailed, and by season, as ads_ml_outcome_solar has them. One row
    per grouping and group, with its days. A day whose sky isn't set yet, its 14 later days not
    being whole, counts under `NOT_YET`: every grouping covers every scored slot once."""
    scored = outcomes[outcomes["is_daylight"].eq(True) & outcomes["error_mw"].notna()]
    scored = scored.assign(
        abs_share=scored["error_share"].abs(),
        abs_mw=scored["error_mw"].abs(),
        curtailed=scored["is_curtailed_day"].map(CURTAILED),
    )
    long = scored.melt(
        id_vars=["target_day", "error_mw", "error_share", "abs_share", "abs_mw"],
        value_vars=["sky", "curtailed", "season"],
        var_name="grouping",
        value_name="group",
    ).fillna({"group": NOT_YET})
    return (
        long.groupby(["grouping", "group"])
        .agg(days=("target_day", "nunique"), **AGGREGATES)
        .reset_index()
    )

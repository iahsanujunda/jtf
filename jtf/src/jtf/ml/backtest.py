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
MEAN_7D_RANGE = "mean_7d_range"
MEAN_7D_SCALED = "mean_7d_scaled"
BOOSTED = "lgbm_quantiles"
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
    "p10_mw",
    "p90_mw",
    "level",
]
# The three forecasts of a slot, low to high: the middle one is the point forecast
QUANTILES = ["p10_mw", "predicted_mw", "p90_mw"]
# What a forecast was made from (plan, Fallback ladder). 0: what its model reads, the day's weather
# among it. 2: the model's fallback, because the day's weather was not there at the decision time.
# Level 1, an older forecast that still covers the day, has nothing to be made from yet
NORMAL, FALLBACK = 0, 2


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
        run_id=run_id,
        model=PERSISTENCE,
        mode=BACKTEST,
        predicted_mw=features["solar_d2_mw"],
        p10_mw=float("nan"),
        p90_mw=float("nan"),
        level=NORMAL,
    )[PREDICTION_COLUMNS]


AGGREGATES = {
    "slots": ("error_mw", "size"),
    "mae_share": ("abs_share", "mean"),
    "bias_share": ("error_share", "mean"),
    "mae_mw": ("abs_mw", "mean"),
    "coverage": ("inside", "mean"),
    "pinball_share": ("pinball_share", "mean"),
    "priced_miss": ("priced_miss", "sum"),
    "price": ("price", "sum"),
}
RANGE = ["in_range", "pinball_share"]  # what only a prediction with a range has
OPTIONAL = [*RANGE, "price_yen_kwh"]  # and what a slot not yet traded lacks


def _scored(outcomes: pd.DataFrame) -> pd.DataFrame:
    """The daylight slots with a label, with what the aggregates read. `coverage` is the share of
    them whose label fell inside the range, to set against the 80% a P10 to P90 range should
    hold; it and the pinball loss are empty for a run with no range."""
    outcomes = outcomes.reindex(columns=outcomes.columns.union(OPTIONAL, sort=False))
    scored = outcomes[outcomes["is_daylight"].eq(True) & outcomes["error_mw"].notna()]
    price = scored["price_yen_kwh"].astype(float)
    return scored.assign(
        abs_share=scored["error_share"].abs(),
        abs_mw=scored["error_mw"].abs(),
        inside=scored["in_range"].astype("boolean").astype("Float64").astype(float),
        pinball_share=scored["pinball_share"].astype(float),
        price=price,
        priced_miss=scored["error_share"].abs() * price,  # empty where the slot has no price
    )


def _priced(aggregated: pd.DataFrame) -> pd.DataFrame:
    """The price-weighted error from its two sums: each slot's miss times the Tokyo price of the
    slot, over the sum of those prices (plan, Metrics). It says how far off the forecast was
    where a megawatt-hour was worth most, and stands beside the plain error, never in its place.
    Empty where no scored slot has a price."""
    weighted = aggregated["priced_miss"] / aggregated["price"].where(aggregated["price"] > 0)
    return aggregated.drop(columns=["priced_miss", "price"]).assign(mae_share_priced=weighted)


def metrics(outcomes: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, float]]:
    """Per fold and overall, over daylight slots with a label (plan, Metrics): MAE and bias as a
    share of the capacity at each decision time (`error_share` from ads_ml_outcome_solar), MAE
    in MW, and for a run with a range its coverage and pinball loss. `outcomes` must come sorted,
    so the means add the same floats in the same order every run.
    """
    scored = _scored(outcomes)
    per_fold = _priced(scored.groupby("fold").agg(**AGGREGATES))
    per_fold = per_fold.dropna(axis=1, how="all").reset_index()
    overall = _priced(scored.groupby(lambda _: "overall").agg(**AGGREGATES)).iloc[0]
    return per_fold, {name: float(value) for name, value in overall.items() if pd.notna(value)}


NOT_YET = "not yet grouped"
CURTAILED = {True: "curtailed", False: "normal"}


def by_group(outcomes: pd.DataFrame) -> pd.DataFrame:
    """The same metrics per kind of day (plan, Segments): by the target day's sky (clear, mixed,
    cloudy), by whether it was curtailed, and by season, as ads_ml_outcome_solar has them. One row
    per grouping and group, with its days. A day whose sky isn't set yet, its 14 later days not
    being whole, counts under `NOT_YET`: every grouping covers every scored slot once."""
    scored = _scored(outcomes)
    scored = scored.assign(curtailed=scored["is_curtailed_day"].map(CURTAILED))
    long = scored.melt(
        id_vars=[
            "target_day",
            "error_mw",
            "error_share",
            "abs_share",
            "abs_mw",
            "inside",
            "pinball_share",
            "price",
            "priced_miss",
        ],
        value_vars=["sky", "curtailed", "season"],
        var_name="grouping",
        value_name="group",
    ).fillna({"group": NOT_YET})
    groups = long.groupby(["grouping", "group"]).agg(days=("target_day", "nunique"), **AGGREGATES)
    return _priced(groups).dropna(axis=1, how="all").reset_index()  # no range, no range scores


BESIDE = {
    "slots": ("abs_share", "size"),
    "mae_share": ("abs_share", "mean"),
    "tepco_mae_share": ("tepco_abs_share", "mean"),
    "bias_share": ("error_share", "mean"),
    "tepco_bias_share": ("tepco_error_share", "mean"),
    "mae_mw": ("abs_mw", "mean"),
    "tepco_mae_mw": ("tepco_abs_mw", "mean"),
}


def benchmark(rows: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, float]]:
    """A run beside TEPCO's own forecast (ads_ml_benchmark_solar): per target day and overall,
    each side's miss as a share of capacity and in MW, over daylight slots. Both are forecasts
    of wind and solar together. TEPCO's is made about ten hours later, so it is a yardstick, not
    a rival. A day counts once every one of its daylight slots has both misses: a day still
    under way would be scored on its morning alone. `days` and `slots` say how much the figures
    stand on; a run that shares no whole day with TEPCO's capture has none, and only those two."""
    lit = rows[rows["is_daylight"].eq(True)]
    both = lit["error_share"].notna() & lit["tepco_error_share"].notna()
    scored = lit[both.groupby(lit["target_day"]).transform("all").astype(bool)]
    if scored.empty:
        return pd.DataFrame(columns=["target_day", *BESIDE]), {"days": 0, "slots": 0}
    scored = scored.assign(
        abs_share=scored["error_share"].abs(),
        tepco_abs_share=scored["tepco_error_share"].abs(),
        abs_mw=scored["error_mw"].abs(),
        tepco_abs_mw=scored["tepco_error_mw"].abs(),
    )
    per_day = scored.groupby("target_day").agg(**BESIDE).reset_index()
    overall = scored.groupby(lambda _: "overall").agg(**BESIDE).iloc[0]
    return per_day, {"days": len(per_day)} | {name: float(v) for name, v in overall.items()}

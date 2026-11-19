"""Trained models: what a run fits on the past and predicts with (jtf.ml.replay runs them).

A trained model gives three forecasts per slot, a low, a middle and a high (P10, P50, P90), keeps
what it learned as text, so that it can be stored and hashed, and says how its training went.
"""

from dataclasses import dataclass

import pandas as pd

from jtf.ml.backtest import QUANTILES


def half_hour(slot_start: pd.Series) -> pd.Series:
    """Each slot's half hour of the day in JST, as `12:30`."""
    return slot_start.dt.tz_convert("Asia/Tokyo").dt.strftime("%H:%M")


@dataclass(frozen=True)
class MissByHour:
    """The seven-day mean with a range around it: per half hour of the day, the 10th, 50th and 90th
    percentile of how far the label fell from the mean, as shares of capacity. The simplest model
    that is trained at all, so that training, its logging and its reproduction can be proven
    before a real model exists; and a range forecast for that model to beat."""

    quantiles: pd.DataFrame  # one row per half hour of the day, QUANTILES as shares of capacity
    summary: dict[str, float]

    def predict(self, features: pd.DataFrame) -> pd.DataFrame:
        miss = self.quantiles.reindex(half_hour(features["slot_start"])).fillna(0.0)
        miss = miss.set_axis(features.index)
        said = miss.mul(features["capacity_mw"], axis=0).add(features["solar_mean_7d_mw"], axis=0)
        return said.clip(lower=0.0)

    def dump(self) -> str:
        return self.quantiles.to_csv(float_format="%.12g")


def train_miss_by_hour(rows: pd.DataFrame) -> MissByHour:
    """Fit MissByHour on daylight slots that have a label and a seven-day mean. Night slots get no
    row, so a prediction for one is the mean itself: zero."""
    known = rows[
        rows["is_daylight"].astype(bool)
        & rows["label_mw"].notna()
        & rows["solar_mean_7d_mw"].notna()
    ]
    miss = (known["label_mw"] - known["solar_mean_7d_mw"]) / known["capacity_mw"]
    when = half_hour(known["slot_start"])
    quantiles = miss.groupby(when).quantile([0.1, 0.5, 0.9]).unstack()
    quantiles.columns = QUANTILES
    quantiles.index.name = "half_hour"
    low, high = (quantiles[side].reindex(when).to_numpy() for side in ("p10_mw", "p90_mw"))
    inside = (miss.to_numpy() >= low) & (miss.to_numpy() <= high)
    summary = {
        "training_slots": float(len(known)),
        "half_hours": float(len(quantiles)),
        "coverage_in_training": float(inside.mean()) if len(known) else float("nan"),
    }
    return MissByHour(quantiles, summary)

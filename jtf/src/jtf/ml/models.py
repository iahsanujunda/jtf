"""Trained models: what a run fits on the past and predicts with (jtf.ml.replay runs them).

A trained model gives three forecasts per slot, a low, a middle and a high (P10, P50, P90), keeps
what it learned as text, so that it can be stored and hashed, and says how its training went.

LightGBM is in the dependency group `ml` and imported only where a model is trained, so the
capture image never carries it.
"""

from dataclasses import dataclass
from typing import Any

import pandas as pd

from jtf.ml.backtest import QUANTILES

# A slot is lit when clear sky allows at least this share of capacity, and a week's forecast is
# worth dividing by from this many W/m²: below either, a ratio divides by almost nothing
LIT_SHARE, WEEK_GHI_FLOOR = 0.05, 20.0


def scaled_mean(features: pd.DataFrame) -> pd.Series:
    """The seven-day mean scaled by how the forecast radiation for the slot compares with its
    mean on the same seven days: the third baseline, and the one to beat (notebook 10, section
    4). Where the slot isn't lit, the week's forecast is too small, or either forecast is
    missing, the plain seven-day mean."""
    week = features["forecast_ghi_mean_7d_w_m2"]
    lit = features["clear_sky_mw"] >= LIT_SHARE * features["capacity_mw"]
    change = (features["forecast_ghi_w_m2"] / week).where(lit & (week >= WEEK_GHI_FLOOR))
    return (features["solar_mean_7d_mw"] * change).fillna(features["solar_mean_7d_mw"])


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


# What the boosted model can be given, and the columns of dws_solar_features each is made from.
# Power is given as a share of capacity, so that a year with more panels reads the same; the
# half hour and the day of the year are read off the slot itself
BOOSTED_INPUTS = {
    "mean_7d_share": ("solar_mean_7d_mw", "capacity_mw"),
    "d2_share": ("solar_d2_mw", "capacity_mw"),
    "clear_sky_share": ("clear_sky_mw", "capacity_mw"),
    "forecast_ghi_w_m2": ("forecast_ghi_w_m2",),
    "forecast_direct_w_m2": ("forecast_direct_w_m2",),
    "forecast_diffuse_w_m2": ("forecast_diffuse_w_m2",),
    "forecast_cloud_cover_pct": ("forecast_cloud_cover_pct",),
    "forecast_temperature_c": ("forecast_temperature_c",),
    "forecast_ghi_mean_7d_w_m2": ("forecast_ghi_mean_7d_w_m2",),
    "half_hour": (),
    "day_of_year": (),
}
# What it is given: every input but two. The day of the year made it worse in 13 of 16 test
# months, and the same slot two days back added a hundredth of a point (notebook 11, section 4)
KEPT_INPUTS = tuple(name for name in BOOSTED_INPUTS if name not in ("d2_share", "day_of_year"))
ALPHAS = dict(zip(QUANTILES, (0.1, 0.5, 0.9), strict=True))
# LightGBM's defaults for everything that shapes the trees (plan, Model and evaluation: no search
# in phase 1). The rest makes a training repeat: one thread, a fixed seed, and the two settings
# its documentation asks for together. The thread count is written into the model's file, so a
# machine with other cores would otherwise give another file for the same trees (notebook 11,
# section 1)
BOOSTED_SETTINGS = {
    "objective": "quantile",
    "deterministic": True,
    "force_row_wise": True,
    "seed": 0,
    "num_threads": 1,
    "verbose": -1,
}
ROUNDS = 100


def columns_of(inputs: tuple[str, ...]) -> tuple[str, ...]:
    """The columns of dws_solar_features those inputs are made from, each once, and the capacity
    that turns a forecast share back into MW."""
    needed = [column for name in inputs for column in BOOSTED_INPUTS[name]]
    return tuple(dict.fromkeys([*needed, "capacity_mw"]))


def boosted_inputs(features: pd.DataFrame, inputs: tuple[str, ...]) -> pd.DataFrame:
    """`inputs` for each row of `features`, as numbers, in the order given."""
    local = features["slot_start"].dt.tz_convert("Asia/Tokyo")
    made = {
        "mean_7d_share": lambda: features["solar_mean_7d_mw"] / features["capacity_mw"],
        "d2_share": lambda: features["solar_d2_mw"] / features["capacity_mw"],
        "clear_sky_share": lambda: features["clear_sky_mw"] / features["capacity_mw"],
        "half_hour": lambda: local.dt.hour * 2 + local.dt.minute // 30,
        "day_of_year": lambda: local.dt.day_of_year,
    }
    columns = {name: made[name]() if name in made else features[name] for name in inputs}
    return pd.DataFrame(columns, index=features.index).astype(float)


@dataclass(frozen=True)
class Boosted:
    """Three gradient-boosted models, one per forecast (the low, the middle, the high), each
    predicting output as a share of capacity from the same inputs."""

    inputs: tuple[str, ...]
    boosters: tuple[Any, ...]  # a lightgbm.Booster per forecast, in the order of QUANTILES
    summary: dict[str, float]

    def predict(self, features: pd.DataFrame) -> pd.DataFrame:
        """MW per slot: the predicted share times the capacity at the decision time, never below
        zero, and zero at night, where the model was shown nothing."""
        given = boosted_inputs(features, self.inputs)
        named = zip(QUANTILES, self.boosters, strict=True)
        shares = {name: booster.predict(given) for name, booster in named}
        said = pd.DataFrame(shares, index=features.index).mul(features["capacity_mw"], axis=0)
        return said.clip(lower=0.0).where(features["is_daylight"].astype(bool), 0.0)

    def dump(self) -> str:
        named = zip(QUANTILES, self.boosters, strict=True)
        return "\n".join(f"# {name}\n{booster.model_to_string()}" for name, booster in named)


def train_boosted(rows: pd.DataFrame, inputs: tuple[str, ...] = KEPT_INPUTS) -> Boosted:
    """Fit Boosted on the daylight slots that have a label. An input may be missing in a row:
    LightGBM sends a missing value down the side of each split that fitted the training rows
    better."""
    import lightgbm

    known = rows[rows["is_daylight"].astype(bool) & rows["label_mw"].notna()]
    given = boosted_inputs(known, inputs)
    share = (known["label_mw"] / known["capacity_mw"]).astype(float)
    boosters = tuple(
        lightgbm.train(BOOSTED_SETTINGS | {"alpha": alpha}, lightgbm.Dataset(given, share), ROUNDS)
        for alpha in ALPHAS.values()
    )
    low, middle, high = (booster.predict(given) for booster in boosters)
    gain = pd.Series(boosters[1].feature_importance("gain"), index=list(inputs))
    summary = {
        "training_slots": float(len(known)),
        "mae_share_in_training": float((share - middle).abs().mean()),
        "coverage_in_training": float(((share >= low) & (share <= high)).mean()),
        **{f"gain_share/{name}": float(value) for name, value in (gain / gain.sum()).items()},
    }
    return Boosted(tuple(inputs), boosters, summary)

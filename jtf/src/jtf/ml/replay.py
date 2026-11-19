"""Runs of a model over decision times, each recorded in `ads_ml_runs`: the time machine's mechanism
(plan, Time machine). One mechanism for every mode:

- `backtest`: the folds of a rolling backtest, scored afterwards by `ml/solar_backtest_metrics`
- `what-if`: any range of target days, with the code checked out now
- `reproduce`: an earlier run again, which must match it exactly

A model is a rule over the features, or is trained: then a run trains it once per month of target
days, on the labelled days known when that month began, and predicts the month from that
training. A reproduce trains again; no stored file is ever read back (plan, Decisions).

A run reads the features its model names, for its target days, from DWS (as known at each decision
time: DWS reads DWD through `as_of`), predicts, appends its predictions to ADS under its run ID, and
records itself: its code, its fidelity per decision time (`dws_fidelity`, counted over the inputs
its model reads) and fingerprints of the features it read and the predictions it wrote, the function
the ODS check uses. A reproduce compares both: different inputs mean the data changed, different
outputs with the same inputs mean the code did. Until images exist, a run's code is its git commit,
so a reproduce refuses on another commit or a tree with uncommitted changes. A run that fails
records nothing; Dagster keeps the failure.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Protocol

import numpy as np
import pandas as pd

from jtf.ml.backtest import (
    BOOSTED,
    MEAN_7D,
    MEAN_7D_RANGE,
    MEAN_7D_SCALED,
    PERSISTENCE,
    PREDICTION_COLUMNS,
    QUANTILES,
)
from jtf.ml.models import KEPT_INPUTS, columns_of, scaled_mean, train_boosted, train_miss_by_hour

MODES = ("backtest", "what-if", "reproduce")
RUN_COLUMNS = [
    "run_id",
    "mode",
    "reproduces",
    "model",
    "model_version",
    "git_commit",
    "tree_dirty",
    "image_digest",
    "first_decision_time",
    "last_decision_time",
    "decision_times",
    "exact",
    "approximate",
    "fault",
    "inputs_fingerprint",
    "outputs_fingerprint",
    "training_fingerprint",
    "started_at",
    "finished_at",
]


class Trained(Protocol):
    """What a model learned from one training."""

    summary: dict[str, float]  # how the training went, for its log

    def predict(self, features: pd.DataFrame) -> pd.DataFrame:
        """QUANTILES for each row of `features`, on its index."""
        ...

    def dump(self) -> str:
        """Everything it learned, as text: kept with the run and hashed."""
        ...


@dataclass(frozen=True)
class Model:
    """A rule over the features (`predict`), or a model trained on the past (`train`)."""

    name: str
    version: str
    features: tuple[str, ...]  # the columns of dws_solar_features it reads, and no others
    inputs: tuple[str, ...]  # what those are made from, as dws_fidelity names them
    predict: Callable[[pd.DataFrame], pd.Series] | None = None
    train: Callable[[pd.DataFrame], Trained] | None = None


MODELS = {
    # The same slot on D−2, the newest day complete at T: the baseline every model must beat
    PERSISTENCE: Model(
        PERSISTENCE, "1", ("solar_d2_mw",), ("actuals_d2",), lambda f: f["solar_d2_mw"]
    ),
    # The mean of the same slot over the seven days D−8 to D−2: the stronger baseline. Clear sky
    # times a recent ratio gives the same forecast (notebook 09, section 5)
    MEAN_7D: Model(
        MEAN_7D, "1", ("solar_mean_7d_mw",), ("actuals_7d",), lambda f: f["solar_mean_7d_mw"]
    ),
    # The seven-day mean scaled by the forecast's change against that week: the one to beat
    MEAN_7D_SCALED: Model(
        MEAN_7D_SCALED,
        "1",
        (
            "solar_mean_7d_mw",
            "forecast_ghi_w_m2",
            "forecast_ghi_mean_7d_w_m2",
            "clear_sky_mw",
            "capacity_mw",
        ),
        ("actuals_7d", "weather", "capacity"),
        scaled_mean,
    ),
    # The seven-day mean with a range learned from its past misses: the simplest trained model
    MEAN_7D_RANGE: Model(
        MEAN_7D_RANGE,
        "1",
        ("solar_mean_7d_mw", "capacity_mw"),
        ("actuals_7d", "capacity"),
        train=train_miss_by_hour,
    ),
    # Three gradient-boosted models, a low, a middle and a high, given the forecast for the slot,
    # the past week and the sun's place: the model (notebook 11)
    BOOSTED: Model(
        BOOSTED,
        "1",
        columns_of(KEPT_INPUTS),
        ("actuals_7d", "weather", "capacity"),
        train=train_boosted,
    ),
}


def training_cutoff(month: date) -> date:
    """The newest target day a model used from the first day of `month` may learn from: two days
    before the month starts. At that first day's decision time, 09:00 the day before, the newest
    day whose labels are all known is two days back (notebook 04, section 5.3)."""
    return month - timedelta(days=2)


def target_day(decision_time) -> date:
    """A decision time's target day: T is 09:00 JST on D−1."""
    return pd.Timestamp(decision_time).tz_convert("Asia/Tokyo").date() + timedelta(days=1)


class Refused(Exception):
    """The run was not made: what was asked can't be done, or not honestly."""


class Mismatch(Exception):
    """A reproduce gave something else than the run it reproduces."""


class RunWarehouse(Protocol):
    def features(self, first_day: date, last_day: date, columns: tuple[str, ...]) -> pd.DataFrame:
        """The key columns, the fold and `columns` of dws_solar_features for the target days, and
        whether each slot is in daylight."""
        ...

    def inputs_fingerprint(
        self, first_day: date, last_day: date, columns: tuple[str, ...]
    ) -> str: ...

    def append_predictions(self, rows: pd.DataFrame) -> None: ...
    def outputs_fingerprint(self, run_id: str) -> str: ...
    def training(self, last_day: date, columns: tuple[str, ...]) -> pd.DataFrame:
        """Every labelled slot of the target days up to `last_day`: the keys, `columns`, the
        label and whether the slot is daylight, sorted."""
        ...

    def training_fingerprints(
        self, last_days: list[date], columns: tuple[str, ...]
    ) -> dict[date, str]:
        """Per last day, the fingerprint of what `training` gives up to it."""
        ...

    def fidelity(self, first_day: date, last_day: date, inputs: tuple[str, ...]) -> pd.Series:
        """Per decision time of the target days, whether every input is exact."""
        ...

    def append_run(self, record: pd.DataFrame) -> None: ...
    def run(self, run_id: str) -> pd.DataFrame: ...


def run(
    warehouse: RunWarehouse,
    *,
    run_id: str,
    mode: str,
    model: str,
    first_day: date,
    last_day: date,
    code: dict[str, str],
    now: Callable[[], datetime],
    fault: str | None = None,
    reproduces: str | None = None,
) -> dict:
    """Predict every decision time of the target days `first_day` to `last_day`, append the
    predictions and the run's record, and return the record."""
    if mode not in MODES:
        raise Refused(f"mode {mode!r} is not one of {MODES}")
    if model not in MODELS:
        raise Refused(f"no model {model!r}: {sorted(MODELS)}")
    if fault:
        raise Refused("no fault presets yet: they come with step 8")
    started = now()
    spec = MODELS[model]
    features = warehouse.features(first_day, last_day, spec.features)
    if features.empty:
        raise Refused(f"no features for target days {first_day} to {last_day}")
    if spec.train:
        said, trainings, known = _train_and_predict(warehouse, spec, features)
        training_fingerprint = trainings[-1]["training_fingerprint"]  # the last month's: all of it
    else:
        said = pd.DataFrame({"predicted_mw": spec.predict(features)}).reindex(columns=QUANTILES)
        trainings, known, training_fingerprint = [], None, None
    rows = features.assign(run_id=run_id, model=spec.name, mode=mode, **said)[PREDICTION_COLUMNS]
    warehouse.append_predictions(rows)
    exact = warehouse.fidelity(first_day, last_day, spec.inputs)
    record = {
        "run_id": run_id,
        "mode": mode,
        "reproduces": reproduces,
        "model": spec.name,
        "model_version": spec.version,
        "git_commit": code.get("git_commit"),
        "tree_dirty": code.get("git_dirty") == "true",
        "image_digest": code.get("image_digest") or None,
        "first_decision_time": features["decision_time"].min(),
        "last_decision_time": features["decision_time"].max(),
        "decision_times": features["decision_time"].nunique(),
        "exact": int(exact.sum()),
        "approximate": int((~exact).sum()),
        "fault": fault,
        "inputs_fingerprint": warehouse.inputs_fingerprint(first_day, last_day, spec.features),
        "outputs_fingerprint": warehouse.outputs_fingerprint(run_id),
        "training_fingerprint": training_fingerprint,
        "started_at": started,
        "finished_at": now(),
    }
    warehouse.append_run(pd.DataFrame([record], columns=RUN_COLUMNS))
    return record | {"trainings": trainings, "training_rows": known}


def _train_and_predict(
    warehouse: RunWarehouse, spec: Model, features: pd.DataFrame
) -> tuple[pd.DataFrame, list[dict], pd.DataFrame]:
    """The model trained month by month on what the warehouse knew, and its predictions. Returns
    them with what each training was, its training set's fingerprint included, and the rows all
    of them learned from: each month's set is those rows up to its last day."""
    learned_until = training_cutoff(max(features["fold"]))
    known = warehouse.training(learned_until, spec.features)
    try:
        said, trainings = train_by_month(spec.train, known, features)
    except Refused as e:
        raise Refused(f"{spec.name}: {e}") from e
    last_days = [training["last_training_day"] for training in trainings]
    fingerprints = warehouse.training_fingerprints(last_days, spec.features)
    trainings = [
        training | {"training_fingerprint": fingerprints[training["last_training_day"]]}
        for training in trainings
    ]
    return said, trainings, known


def train_by_month(
    train: Callable[[pd.DataFrame], Trained], known: pd.DataFrame, features: pd.DataFrame
) -> tuple[pd.DataFrame, list[dict]]:
    """One training per month of target days in `features`, each on the rows of `known` up to
    that month's cutoff, and that month's predictions from it. The three forecasts of a slot are
    sorted, so that a low never lies above a high. Returns the predictions on `features`' index,
    and what each training was: its month, its last day, its size, its file and the file's hash,
    its summary."""
    parts, trainings = [], []
    for month in sorted(set(features["fold"])):  # a model each: a handful of months, not rows
        past = known[known["target_day"] <= training_cutoff(month)]
        if past.empty:
            raise Refused(f"nothing to train on before {month}")
        learned = train(past)
        asked = features[features["fold"] == month]
        said = learned.predict(asked)[QUANTILES].to_numpy(float)
        parts.append(pd.DataFrame(np.sort(said, axis=1), index=asked.index, columns=QUANTILES))
        file = learned.dump()
        trainings.append(
            {
                "fold": month,
                "last_training_day": training_cutoff(month),
                "rows": len(past),
                "sha256": hashlib.sha256(file.encode()).hexdigest(),
                "file": file,
                **learned.summary,
            }
        )
    return pd.concat(parts).reindex(features.index), trainings


def reproduce(
    warehouse: RunWarehouse,
    *,
    run_id: str,
    original: str,
    code: dict[str, str],
    now: Callable[[], datetime],
) -> dict:
    """Run `original` again with the same code, and fail unless its inputs and outputs match."""
    found = warehouse.run(original)
    if found.empty:
        raise Refused(f"no run {original!r} in ads_ml_runs")
    then = found.iloc[0]
    if then["tree_dirty"]:
        raise Refused(f"run {original} came from uncommitted code: there is nothing to reproduce")
    if code.get("git_dirty") == "true" or code.get("git_commit") != then["git_commit"]:
        raise Refused(f"run {original} came from commit {then['git_commit']}: check it out, clean")
    record = run(
        warehouse,
        run_id=run_id,
        mode="reproduce",
        model=then["model"],
        first_day=target_day(then["first_decision_time"]),
        last_day=target_day(then["last_decision_time"]),
        code=code,
        now=now,
        reproduces=original,
    )
    differ = [side for side in CAUSES if not _same(record.get(side), then.get(side))]
    if differ:
        raise Mismatch(
            f"run {run_id} does not reproduce {original}: {', '.join(differ)} ({CAUSES[differ[0]]})"
        )
    return record


# What a reproduce compares, in the order a difference is blamed: the features it read, what it
# was trained on, and only then its predictions, which the same inputs can change only by code
CAUSES = {
    "inputs_fingerprint": "the data changed",
    "training_fingerprint": "the training data changed",
    "outputs_fingerprint": "the code did",
}


def _same(now: object, then: object) -> bool:
    """Equal, or both missing: a model that isn't trained has no training fingerprint."""
    return (pd.isna(now) and pd.isna(then)) or now == then

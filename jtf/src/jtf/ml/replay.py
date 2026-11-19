"""Runs of a model over decision times, each recorded in `ads_ml_runs`: the time machine's mechanism
(plan, Time machine). One mechanism for every mode:

- `backtest`: the folds of a rolling backtest, scored afterwards by `ml/solar_backtest_metrics`
- `what-if`: any range of target days, with the code checked out now
- `reproduce`: an earlier run again, which must match it exactly

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

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Protocol

import pandas as pd

from jtf.ml.backtest import MEAN_7D, PERSISTENCE, PREDICTION_COLUMNS

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
    "started_at",
    "finished_at",
]


@dataclass(frozen=True)
class Model:
    name: str
    version: str
    features: tuple[str, ...]  # the columns of dws_solar_features it reads, and no others
    inputs: tuple[str, ...]  # what those are made from, as dws_fidelity names them
    predict: Callable[[pd.DataFrame], pd.Series]


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
}


def target_day(decision_time) -> date:
    """A decision time's target day: T is 09:00 JST on D−1."""
    return pd.Timestamp(decision_time).tz_convert("Asia/Tokyo").date() + timedelta(days=1)


class Refused(Exception):
    """The run was not made: what was asked can't be done, or not honestly."""


class Mismatch(Exception):
    """A reproduce gave something else than the run it reproduces."""


class RunWarehouse(Protocol):
    def features(self, first_day: date, last_day: date, columns: tuple[str, ...]) -> pd.DataFrame:
        """The key columns, the fold and `columns` of dws_solar_features for the target days."""
        ...

    def inputs_fingerprint(
        self, first_day: date, last_day: date, columns: tuple[str, ...]
    ) -> str: ...

    def append_predictions(self, rows: pd.DataFrame) -> None: ...
    def outputs_fingerprint(self, run_id: str) -> str: ...
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
    rows = features.assign(
        run_id=run_id, model=spec.name, mode=mode, predicted_mw=spec.predict(features)
    )[PREDICTION_COLUMNS]
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
        "started_at": started,
        "finished_at": now(),
    }
    warehouse.append_run(pd.DataFrame([record], columns=RUN_COLUMNS))
    return record


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
    differ = [
        side for side in ("inputs_fingerprint", "outputs_fingerprint") if record[side] != then[side]
    ]
    if differ:
        cause = "the data changed" if "inputs_fingerprint" in differ else "the code did"
        raise Mismatch(f"run {run_id} does not reproduce {original}: {', '.join(differ)} ({cause})")
    return record

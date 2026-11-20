"""A model's runs (jtf.ml.replay): recorded with their code, fidelity and fingerprints, and a
reproduce that matches its original exactly or says what changed."""

from datetime import UTC, date, datetime

import pandas as pd
import pytest

from jtf.ml.backtest import MEAN_7D, PERSISTENCE
from jtf.ml.replay import MODELS, Mismatch, Model, Refused, reproduce, run, target_day
from jtf.ml.warehouse import KEY_COLUMNS, OUTPUT_COLUMNS

CODE = {"git_commit": "abc123", "git_dirty": "false", "image_digest": ""}
NOW = lambda: datetime(2026, 11, 18, 12, 0, tzinfo=UTC)  # noqa: E731


def fingerprint(rows: pd.DataFrame, columns: list[str]) -> str:
    """Per-row hashes summed: the same rows in any order give the same value, as in BigQuery."""
    return str(int(pd.util.hash_pandas_object(rows[columns], index=False).astype(object).sum()))


class FakeRunWarehouse:
    def __init__(self, features: pd.DataFrame, exact: pd.Series):
        self.feature_rows, self.exact = features, exact
        self.predictions: list[pd.DataFrame] = []
        self.runs: list[pd.DataFrame] = []

    def _days(self, rows, first_day, last_day):
        return rows[(rows["target_day"] >= first_day) & (rows["target_day"] <= last_day)]

    def features(self, first_day, last_day, columns):
        rows = self._days(self.feature_rows, first_day, last_day)
        return rows[["fold", *KEY_COLUMNS, *columns]].reset_index(drop=True)

    def inputs_fingerprint(self, first_day, last_day, columns):
        rows = self._days(self.feature_rows, first_day, last_day)
        return fingerprint(rows, [*KEY_COLUMNS, *columns])

    def append_predictions(self, rows):
        self.predictions.append(rows)

    def outputs_fingerprint(self, run_id):
        rows = pd.concat(self.predictions)
        return fingerprint(rows[rows["run_id"] == run_id], OUTPUT_COLUMNS)

    def fidelity(self, first_day, last_day, inputs):
        self.fidelity_asked_of = inputs
        return self.exact

    def append_run(self, record):
        self.runs.append(record)

    def run(self, run_id):
        found = pd.concat(self.runs) if self.runs else pd.DataFrame(columns=["run_id"])
        return found[found["run_id"] == run_id].reset_index(drop=True)


def decision(day: str) -> pd.Timestamp:
    """09:00 JST on `day`, the decision time for the next day."""
    return pd.Timestamp(f"{day} 09:00", tz="Asia/Tokyo").tz_convert("UTC")


@pytest.fixture
def warehouse():
    times = [decision("2026-11-16"), decision("2026-11-17")]
    features = pd.DataFrame(
        {
            "fold": [date(2026, 11, 1)] * 4,
            "decision_time": [times[0], times[0], times[1], times[1]],
            "target_day": [date(2026, 11, 17)] * 2 + [date(2026, 11, 18)] * 2,
            "slot_start": pd.to_datetime(
                ["2026-11-17 03:00", "2026-11-17 03:30", "2026-11-18 03:00", "2026-11-18 03:30"],
                utc=True,
            ),
            "solar_d2_mw": [5000.0, 5100.0, 6000.0, None],
            "solar_mean_7d_mw": [4000.0, 4100.0, 4200.0, 4300.0],
        }
    )
    exact = pd.Series([False, True], index=times)  # the 16th's D−2 was partly rule-dated
    return FakeRunWarehouse(features, exact)


def what_if(warehouse, run_id="r1", **kwargs):
    args = {
        "run_id": run_id,
        "mode": "what-if",
        "model": PERSISTENCE,
        "first_day": date(2026, 11, 17),
        "last_day": date(2026, 11, 18),
        "code": CODE,
        "now": NOW,
    }
    return run(warehouse, **(args | kwargs))


def test_a_run_appends_its_predictions_and_one_record_with_its_code_and_fidelity(warehouse):
    record = what_if(warehouse)

    (predictions,) = warehouse.predictions
    assert list(predictions["predicted_mw"].fillna(-1)) == [5000.0, 5100.0, 6000.0, -1]
    assert set(predictions["mode"]) == {"what-if"} and set(predictions["run_id"]) == {"r1"}
    (stored,) = warehouse.runs
    assert stored.iloc[0]["run_id"] == "r1" and len(stored) == 1
    assert record["model"] == PERSISTENCE and record["model_version"] == "1"
    assert record["git_commit"] == "abc123" and record["tree_dirty"] is False
    assert record["decision_times"] == 2 and (record["exact"], record["approximate"]) == (1, 1)
    assert record["inputs_fingerprint"] and record["outputs_fingerprint"]


def test_each_model_predicts_from_its_own_feature_and_fingerprints_only_what_it_reads(warehouse):
    days = {"mode": "what-if", "first_day": date(2026, 11, 17), "last_day": date(2026, 11, 18)}
    before = {
        model: run(warehouse, run_id=f"{model}-1", model=model, code=CODE, now=NOW, **days)
        for model in (PERSISTENCE, MEAN_7D)
    }
    mean = warehouse.predictions[-1]
    assert list(mean["predicted_mw"]) == [4000.0, 4100.0, 4200.0, 4300.0]
    assert set(mean["model"]) == {MEAN_7D} and before[MEAN_7D]["model_version"] == "1"

    warehouse.feature_rows.loc[0, "solar_d2_mw"] = 4999.0  # only persistence reads this
    after = {
        model: run(warehouse, run_id=f"{model}-2", model=model, code=CODE, now=NOW, **days)
        for model in (PERSISTENCE, MEAN_7D)
    }
    assert after[MEAN_7D]["inputs_fingerprint"] == before[MEAN_7D]["inputs_fingerprint"]
    assert after[PERSISTENCE]["inputs_fingerprint"] != before[PERSISTENCE]["inputs_fingerprint"]


@pytest.mark.parametrize(
    ("model", "inputs"), [(PERSISTENCE, ("actuals_d2",)), (MEAN_7D, ("actuals_7d",))]
)
def test_a_run_is_exact_only_where_the_inputs_its_model_reads_are(warehouse, model, inputs):
    days = {"first_day": date(2026, 11, 17), "last_day": date(2026, 11, 18)}
    run(warehouse, run_id="r1", mode="what-if", model=model, code=CODE, now=NOW, **days)
    assert warehouse.fidelity_asked_of == inputs


def test_the_target_day_is_the_day_after_the_decision_time_in_jst():
    assert target_day(decision("2026-11-16")) == date(2026, 11, 17)


@pytest.mark.parametrize(
    "asked, why",
    [
        ({"mode": "replay"}, "mode"),
        ({"model": "m1"}, "no model"),
        ({"fault": "F9"}, "no fault 'F9'"),
        ({"first_day": date(2026, 12, 1), "last_day": date(2026, 12, 2)}, "no features"),
    ],
    ids=["unknown mode", "unknown model", "a fault that doesn't exist", "no features"],
)
def test_a_run_is_refused_before_it_writes_anything(warehouse, asked, why):
    with pytest.raises(Refused, match=why):
        what_if(warehouse, **asked)
    assert not warehouse.predictions and not warehouse.runs


def test_two_runs_of_the_same_days_have_the_same_fingerprints_whatever_their_run_id(warehouse):
    first, second = what_if(warehouse, "r1"), what_if(warehouse, "r2", mode="backtest")

    assert first["inputs_fingerprint"] == second["inputs_fingerprint"]
    assert first["outputs_fingerprint"] == second["outputs_fingerprint"]


def test_a_reproduce_matches_its_original_and_names_it(warehouse):
    what_if(warehouse, "r1")

    record = reproduce(warehouse, run_id="r2", original="r1", code=CODE, now=NOW)

    assert record["mode"] == "reproduce" and record["reproduces"] == "r1"
    assert record["decision_times"] == 2


@pytest.mark.parametrize(
    "then, now, why",
    [
        (CODE, CODE | {"git_commit": "def456"}, "commit abc123"),
        (CODE, CODE | {"git_dirty": "true"}, "commit abc123"),
        (CODE | {"git_dirty": "true"}, CODE, "uncommitted"),
    ],
    ids=["another commit", "uncommitted changes now", "an original from uncommitted code"],
)
def test_a_reproduce_is_refused_on_other_code(warehouse, then, now, why):
    what_if(warehouse, "r1", code=then)

    with pytest.raises(Refused, match=why):
        reproduce(warehouse, run_id="r2", original="r1", code=now, now=NOW)


def test_a_reproduce_of_an_unknown_run_is_refused(warehouse):
    with pytest.raises(Refused, match="no run"):
        reproduce(warehouse, run_id="r2", original="nope", code=CODE, now=NOW)


def test_a_reproduce_says_when_the_data_changed(warehouse):
    what_if(warehouse, "r1")
    warehouse.feature_rows.loc[0, "solar_d2_mw"] = 4999.0  # an input known at T changed

    with pytest.raises(Mismatch, match="the data changed"):
        reproduce(warehouse, run_id="r2", original="r1", code=CODE, now=NOW)


def test_a_reproduce_says_when_the_code_changed(warehouse, monkeypatch):
    what_if(warehouse, "r1")
    changed = Model(
        PERSISTENCE, "1", ("solar_d2_mw",), ("actuals_d2",), lambda f: f["solar_d2_mw"] * 1.01
    )
    monkeypatch.setitem(MODELS, PERSISTENCE, changed)

    with pytest.raises(Mismatch, match="the code did"):
        reproduce(warehouse, run_id="r2", original="r1", code=CODE, now=NOW)

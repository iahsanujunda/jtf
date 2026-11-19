"""Trained models in a run (jtf.ml.replay, jtf.ml.models): one training per month on what was
known when the month began, three sorted forecasts per slot, and a reproduce that trains again."""

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

import pandas as pd
import pytest

from jtf.ml.backtest import MEAN_7D, MEAN_7D_RANGE, QUANTILES
from jtf.ml.models import half_hour, scaled_mean, train_miss_by_hour
from jtf.ml.replay import MODELS, Mismatch, Model, Refused, reproduce, run, training_cutoff
from jtf.ml.warehouse import KEY_COLUMNS, OUTPUT_COLUMNS

CODE = {"git_commit": "abc123", "git_dirty": "false", "image_digest": ""}
NOW = lambda: datetime(2026, 11, 18, 12, 0, tzinfo=UTC)  # noqa: E731
JST = "Asia/Tokyo"


def fingerprint(rows: pd.DataFrame, columns: list[str]) -> str:
    return str(int(pd.util.hash_pandas_object(rows[columns], index=False).astype(object).sum()))


def world() -> pd.DataFrame:
    """One slot a day at 12:00 JST from 1 January to 31 March 2026: capacity 10,000 MW, a
    seven-day mean of 5,000 MW, and a label that is the mean plus 100 MW per month of the year,
    so each month's labels tell on whoever read them."""
    days = pd.date_range("2026-01-01", "2026-03-31", freq="D")
    slots = (days + pd.Timedelta(hours=12)).tz_localize(JST).tz_convert("UTC")
    return pd.DataFrame(
        {
            "fold": [d.replace(day=1).date() for d in days],
            "decision_time": slots - pd.Timedelta(hours=27),
            "target_day": [d.date() for d in days],
            "slot_start": slots,
            "solar_mean_7d_mw": 5000.0,
            "capacity_mw": 10000.0,
            "label_mw": 5000.0 + 100.0 * days.month,
            "is_daylight": True,
        }
    )


class FakeWarehouse:
    def __init__(self, rows: pd.DataFrame):
        self.rows = rows
        self.predictions: list[pd.DataFrame] = []
        self.runs: list[pd.DataFrame] = []

    def _days(self, first_day, last_day):
        return self.rows[self.rows["target_day"].between(first_day, last_day)]

    def features(self, first_day, last_day, columns):
        rows = self._days(first_day, last_day)
        return rows[["fold", *KEY_COLUMNS, *columns]].reset_index(drop=True)

    def inputs_fingerprint(self, first_day, last_day, columns):
        return fingerprint(self._days(first_day, last_day), [*KEY_COLUMNS, *columns])

    def training(self, last_day, columns):
        rows = self.rows[self.rows["target_day"] <= last_day]
        return rows[[*KEY_COLUMNS, *columns, "label_mw", "is_daylight"]].reset_index(drop=True)

    def training_fingerprint(self, last_day, columns):
        return fingerprint(self.training(last_day, columns), [*columns, "label_mw"])

    def append_predictions(self, rows):
        self.predictions.append(rows)

    def outputs_fingerprint(self, run_id):
        rows = pd.concat(self.predictions)
        return fingerprint(rows[rows["run_id"] == run_id], OUTPUT_COLUMNS)

    def fidelity(self, first_day, last_day, inputs):
        times = self._days(first_day, last_day)["decision_time"].unique()
        return pd.Series(False, index=times)

    def append_run(self, record):
        self.runs.append(record)

    def run(self, run_id):
        found = pd.concat(self.runs) if self.runs else pd.DataFrame(columns=["run_id"])
        return found[found["run_id"] == run_id].reset_index(drop=True)


@pytest.fixture
def warehouse():
    return FakeWarehouse(world())


def a_run(warehouse, run_id="r1", first=date(2026, 2, 1), last=date(2026, 3, 31), **kwargs):
    args = {"mode": "backtest", "model": MEAN_7D_RANGE, "code": CODE, "now": NOW}
    return run(warehouse, run_id=run_id, first_day=first, last_day=last, **(args | kwargs))


def test_a_model_used_from_a_months_first_day_learns_up_to_two_days_before_it():
    assert training_cutoff(date(2026, 3, 1)) == date(2026, 2, 27)


def test_each_month_is_predicted_by_a_model_trained_on_what_was_known_when_it_began(warehouse):
    record = a_run(warehouse)

    # February's model saw January to the 30th only: its miss is January's, +100 MW. March's saw
    # January and February to the 27th: 30 days at +100 and 27 at +200, a median of +100 MW
    (predictions,) = warehouse.predictions
    by_month = predictions.groupby("fold")["predicted_mw"].unique().map(list).to_dict()
    assert by_month == {date(2026, 2, 1): [5100.0], date(2026, 3, 1): [5100.0]}
    trainings = pd.DataFrame(record["trainings"]).set_index("fold")
    assert trainings["last_training_day"].to_dict() == {
        date(2026, 2, 1): date(2026, 1, 30),
        date(2026, 3, 1): date(2026, 2, 27),
    }
    assert trainings["rows"].to_dict() == {date(2026, 2, 1): 30, date(2026, 3, 1): 58}
    # March's range reaches February's higher labels; February's can't
    highs = predictions.groupby("fold")["p90_mw"].max().to_dict()
    assert highs[date(2026, 2, 1)] == 5100.0 and highs[date(2026, 3, 1)] == 5200.0


def test_every_training_keeps_its_file_and_the_files_hash(warehouse):
    record = a_run(warehouse)

    files = {t["fold"]: (t["file"], t["sha256"]) for t in record["trainings"]}
    assert "12:00" in files[date(2026, 2, 1)][0]
    assert len({sha for _, sha in files.values()}) == 2 and all(
        len(s) == 64 for _, s in files.values()
    )
    assert record["trainings"][0]["training_slots"] == 30


def test_a_low_never_lies_above_a_high_whatever_the_model_says(warehouse, monkeypatch):
    @dataclass
    class Crossed:
        summary: dict

        def predict(self, features):
            return pd.DataFrame(
                {"p10_mw": 900.0, "predicted_mw": 100.0, "p90_mw": 500.0}, index=features.index
            )

        def dump(self):
            return "crossed"

    crossed = Model("crossed", "1", ("solar_mean_7d_mw",), (), train=lambda rows: Crossed({}))
    monkeypatch.setitem(MODELS, "crossed", crossed)
    a_run(warehouse, model="crossed")

    (predictions,) = warehouse.predictions
    assert predictions[QUANTILES].drop_duplicates().to_numpy().tolist() == [[100.0, 500.0, 900.0]]


def test_a_rule_is_not_trained_and_gives_no_range(warehouse):
    record = a_run(warehouse, model=MEAN_7D)

    (predictions,) = warehouse.predictions
    assert predictions["predicted_mw"].eq(5000.0).all()
    assert predictions[["p10_mw", "p90_mw"]].isna().all().all()
    assert record["training_fingerprint"] is None and record["trainings"] == []


def test_a_run_with_nothing_to_train_on_is_refused_before_it_writes_anything(warehouse):
    with pytest.raises(Refused, match="nothing to train"):
        a_run(warehouse, first=date(2026, 1, 1), last=date(2026, 1, 31))
    assert not warehouse.predictions and not warehouse.runs


def test_a_reproduce_trains_again_and_matches(warehouse):
    original = a_run(warehouse, "r1")

    record = reproduce(warehouse, run_id="r2", original="r1", code=CODE, now=NOW)

    assert record["training_fingerprint"] == original["training_fingerprint"] is not None
    assert [t["sha256"] for t in record["trainings"]] == [
        t["sha256"] for t in original["trainings"]
    ]


def test_a_reproduce_says_when_a_label_it_was_trained_on_changed(warehouse):
    a_run(warehouse, "r1")
    revised = warehouse.rows["target_day"] == date(2026, 1, 10)
    warehouse.rows.loc[revised, "label_mw"] += 3000.0  # a revision of a day in every training

    with pytest.raises(Mismatch, match="the training data changed"):
        reproduce(warehouse, run_id="r2", original="r1", code=CODE, now=NOW)


def test_the_miss_is_learned_per_half_hour_from_daylight_slots_with_a_label():
    slot = pd.Timestamp("2026-01-01 12:00", tz=JST).tz_convert("UTC")
    rows = pd.DataFrame(
        {
            # 12:00 on ten days: misses of 0 to 900 MW; one night slot and one unlabelled slot
            "slot_start": [slot + timedelta(days=d) for d in range(10)]
            + [slot + timedelta(hours=12), slot + timedelta(minutes=30)],
            "solar_mean_7d_mw": [5000.0] * 10 + [0.0, 5000.0],
            "capacity_mw": 10000.0,
            "label_mw": [5000.0 + 100.0 * d for d in range(10)] + [700.0, None],
            "is_daylight": [True] * 10 + [False, True],
        }
    )
    learned = train_miss_by_hour(rows)

    assert learned.quantiles.index.tolist() == ["12:00"]
    assert learned.quantiles.loc["12:00"].tolist() == pytest.approx([0.009, 0.045, 0.081])
    assert learned.summary["training_slots"] == 10
    assert learned.summary["coverage_in_training"] == pytest.approx(0.8)
    assert half_hour(rows["slot_start"]).iloc[0] == "12:00"


def test_the_range_is_the_mean_plus_the_learned_miss_and_never_below_zero():
    slot = pd.Timestamp("2026-01-01 12:00", tz=JST).tz_convert("UTC")
    rows = pd.DataFrame(
        {
            "slot_start": [slot + timedelta(days=d) for d in range(10)],
            "solar_mean_7d_mw": 5000.0,
            "capacity_mw": 10000.0,
            "label_mw": [4000.0 + 200.0 * d for d in range(10)],  # misses of −1,000 to +800 MW
            "is_daylight": True,
        }
    )
    learned = train_miss_by_hour(rows)
    asked = pd.DataFrame(
        {
            "slot_start": [slot + timedelta(days=30), slot + timedelta(days=30, hours=12)],
            "solar_mean_7d_mw": [500.0, 0.0],  # a dim noon, and midnight, which was never learned
            "capacity_mw": 20000.0,
        }
    )
    said = learned.predict(asked)

    # Shares of capacity: −0.082, −0.01, +0.062, on 20,000 MW around a mean of 500 MW
    assert said.iloc[0].tolist() == pytest.approx([0.0, 300.0, 1740.0])
    assert said.iloc[1].tolist() == [0.0, 0.0, 0.0]
    assert learned.dump() == train_miss_by_hour(rows).dump()


def test_the_scaled_mean_follows_the_forecast_only_where_a_ratio_means_something():
    features = pd.DataFrame(
        {
            # a lit slot forecast brighter than its week; a lit slot forecast duller; a slot that
            # isn't lit; a week too dim to divide by; a week with no forecast; no forecast at all
            "solar_mean_7d_mw": [5000.0, 5000.0, 300.0, 5000.0, 5000.0, 5000.0],
            "forecast_ghi_w_m2": [600.0, 200.0, 60.0, 30.0, 600.0, None],
            "forecast_ghi_mean_7d_w_m2": [400.0, 400.0, 30.0, 10.0, None, 400.0],
            "clear_sky_mw": [8000.0, 8000.0, 400.0, 8000.0, 8000.0, 8000.0],
            "capacity_mw": 10000.0,
        }
    )
    assert scaled_mean(features).tolist() == [7500.0, 2500.0, 300.0, 5000.0, 5000.0, 5000.0]
    assert MODELS["mean_7d_scaled"].inputs == ("actuals_7d", "weather", "capacity")
    assert set(features.columns) == set(MODELS["mean_7d_scaled"].features)

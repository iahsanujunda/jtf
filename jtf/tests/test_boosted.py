"""The boosted model (jtf.ml.models): what it is given, what it learns from, that it gives MW that
scale with capacity and nothing at night, and that a training repeats exactly, here and on any
machine that runs these tests."""

import hashlib
from datetime import date

import numpy as np
import pandas as pd
import pytest

from jtf.ml.backtest import QUANTILES
from jtf.ml.models import (
    BOOSTED_INPUTS,
    KEPT_INPUTS,
    Boosted,
    boosted_inputs,
    columns_of,
    train_boosted,
)
from jtf.ml.replay import train_by_month

CAPACITY = 20_000.0
# The model's file for `world()` under LightGBM 4.7.0 on Linux x86-64, made on a laptop. CI trains
# on another machine: while this holds there, a run can be reproduced on either. LightGBM's
# documentation promises it for the same version and binary only, so an upgrade may change it:
# then say so in the pull request, since earlier runs of the model stop being reproducible
RECORDED = "cc177ec5e9ad319a4bb9c497e17d85d00e65b651aa3e4e25be5f9bb4b3386a16"


def world(days: int = 120) -> pd.DataFrame:
    """Every half hour of `days` days from 2025-01-01, made with whole-number arithmetic only, so
    that every machine builds the same rows to the last bit. The sun is up from 06:00 to 18:00;
    each day has its own cloud, and the label follows clear sky and the forecast."""
    grid = pd.MultiIndex.from_product([range(days), range(48)], names=["day", "slot"]).to_frame()
    day, slot = grid["day"].to_numpy(), grid["slot"].to_numpy()
    midnight = pd.Timestamp("2025-01-01", tz="Asia/Tokyo") + pd.to_timedelta(day, unit="D")
    slot_start = (midnight + pd.to_timedelta(slot * 30, unit="min")).tz_convert("UTC")
    daylight = (slot >= 12) & (slot < 36)
    clear = np.where(daylight, (12 - np.abs(slot - 24)) / 15, 0.0)  # up to 0.8 of capacity
    cloud = (day * 37 + 11) % 100  # percent, another each day
    through = (100 - cloud * 4 // 5) / 100  # what the cloud lets through
    wobble = ((day * 13 + slot * 7) % 11 - 5) / 500
    return pd.DataFrame(
        {
            "decision_time": slot_start.floor("D") - pd.Timedelta(hours=24),
            "target_day": [d.date() for d in midnight],
            "slot_start": slot_start,
            "capacity_mw": CAPACITY,
            "clear_sky_mw": clear * CAPACITY,
            "solar_mean_7d_mw": clear * CAPACITY * 3 / 5,
            "solar_d2_mw": clear * CAPACITY * ((day * 37 + 37) % 100) / 100,
            "forecast_ghi_w_m2": clear * through * 1000,
            "forecast_direct_w_m2": clear * through * 700,
            "forecast_diffuse_w_m2": clear * through * 300,
            "forecast_cloud_cover_pct": cloud.astype(float),
            "forecast_temperature_c": 10.0 + day % 20,
            "forecast_ghi_mean_7d_w_m2": clear * 600,
            "label_mw": np.where(daylight, clear * through * 9 / 10 + wobble, 0.0) * CAPACITY,
            "is_daylight": daylight,
        }
    ).reset_index(drop=True)


@pytest.fixture(scope="module")
def rows() -> pd.DataFrame:
    return world()


@pytest.fixture(scope="module")
def learned(rows) -> Boosted:
    return train_boosted(rows[rows["target_day"] < date(2025, 4, 1)])


def test_an_inputs_columns_are_named_once_with_the_capacity_that_gives_mw():
    assert columns_of(("mean_7d_share", "clear_sky_share", "half_hour")) == (
        "solar_mean_7d_mw",
        "capacity_mw",
        "clear_sky_mw",
    )
    assert columns_of(("forecast_ghi_w_m2",)) == ("forecast_ghi_w_m2", "capacity_mw")
    assert set(columns_of(tuple(BOOSTED_INPUTS))) == {
        column for columns in BOOSTED_INPUTS.values() for column in columns
    }


def test_the_model_keeps_every_input_but_the_day_of_the_year_and_two_days_back(rows):
    left_out = set(BOOSTED_INPUTS) - set(KEPT_INPUTS)
    past = rows[rows["target_day"] < date(2025, 2, 1)]

    assert left_out == {"day_of_year", "d2_share"} and len(KEPT_INPUTS) == 9
    assert train_boosted(past).inputs == KEPT_INPUTS
    assert "solar_d2_mw" not in columns_of(KEPT_INPUTS)  # one less day of actuals to depend on


def test_power_is_given_as_a_share_of_capacity_and_the_calendar_in_jst():
    slots = pd.DataFrame(
        {
            # 12:30 on 1 March in Tokyo, and 00:30 on New Year's Day there: still 31 December in UTC
            "slot_start": pd.to_datetime(["2025-03-01 03:30", "2025-12-31 15:30"], utc=True),
            "capacity_mw": [20_000.0, 25_000.0],
            "solar_mean_7d_mw": [5_000.0, 0.0],
            "solar_d2_mw": [10_000.0, 0.0],
            "clear_sky_mw": [12_000.0, 0.0],
            "forecast_ghi_w_m2": [640.0, None],
        }
    )
    names = (
        "mean_7d_share",
        "d2_share",
        "clear_sky_share",
        "forecast_ghi_w_m2",
        "half_hour",
        "day_of_year",
    )
    given = boosted_inputs(slots, names)

    assert list(given.columns) == list(names)
    assert given.iloc[0].to_list() == [0.25, 0.5, 0.6, 640.0, 25.0, 60.0]
    assert given.iloc[1][["half_hour", "day_of_year"]].to_list() == [1.0, 1.0]
    assert np.isnan(given.iloc[1]["forecast_ghi_w_m2"])


def test_it_learns_from_daylight_slots_with_a_label_and_from_nothing_else(rows):
    past = rows[rows["target_day"] < date(2025, 2, 1)].copy()
    file = train_boosted(past).dump()

    night, lit = past.index[~past["is_daylight"]][0], past.index[past["is_daylight"]][0]
    other_night = past.assign(label_mw=past["label_mw"].where(past.index != night, 9_999.0))
    unlabelled = past.assign(label_mw=past["label_mw"].where(past.index != lit))
    other_day = past.assign(label_mw=past["label_mw"].where(past.index != lit, 9_999.0))

    assert train_boosted(other_night).dump() == file
    assert train_boosted(past).summary["training_slots"] == 31 * 24
    assert train_boosted(unlabelled).summary["training_slots"] == 31 * 24 - 1
    assert train_boosted(other_day).dump() != file  # the same change by day is learned from


def test_it_gives_mw_that_follow_the_forecast_more_closely_than_the_seven_day_mean(rows, learned):
    april = rows[(rows["target_day"] >= date(2025, 4, 1)) & rows["is_daylight"]]
    said = learned.predict(april)

    miss = (said["predicted_mw"] - april["label_mw"]).abs().mean() / CAPACITY
    mean_miss = (april["solar_mean_7d_mw"] - april["label_mw"]).abs().mean() / CAPACITY
    assert list(said.columns) == QUANTILES and said.index.equals(april.index)
    assert miss < 0.02 < 0.05 < mean_miss
    inside = (april["label_mw"] >= said["p10_mw"]) & (april["label_mw"] <= said["p90_mw"])
    assert 0.6 < inside.mean() < 0.95  # a range meant to hold 80%
    assert 0.7 < learned.summary["coverage_in_training"] < 0.9


def test_what_it_learned_is_a_share_so_twice_the_capacity_gives_twice_the_mw(rows, learned):
    april = rows[rows["target_day"] >= date(2025, 4, 1)]
    scaled = ["capacity_mw", "solar_mean_7d_mw", "solar_d2_mw", "clear_sky_mw"]
    doubled = april.assign(**{name: april[name] * 2 for name in scaled})

    said, twice = learned.predict(april), learned.predict(doubled)

    assert said.loc[april["is_daylight"], "predicted_mw"].gt(0).mean() > 0.9
    pd.testing.assert_frame_equal(twice, said * 2)


def test_a_fleet_that_doubled_halfway_teaches_the_same_as_one_that_never_grew(rows):
    past = rows[rows["target_day"] < date(2025, 2, 1)]
    later = past["target_day"] >= date(2025, 1, 16)
    scaled = ["capacity_mw", "solar_mean_7d_mw", "solar_d2_mw", "clear_sky_mw", "label_mw"]
    grown = past.assign(**{name: past[name].where(~later, past[name] * 2) for name in scaled})

    assert grown["label_mw"].max() > past["label_mw"].max() * 1.5
    assert train_boosted(grown).dump() == train_boosted(past).dump()


def test_night_slots_get_zero_and_no_forecast_is_below_zero(rows, learned):
    april = rows[rows["target_day"] >= date(2025, 4, 1)]
    # A model whose every forecast is negative, as a low one can be at dawn
    below = Boosted(learned.inputs, tuple(Sunk(b) for b in learned.boosters), {})

    said = learned.predict(april)

    assert (said[~april["is_daylight"]] == 0.0).all().all()
    assert (said >= 0.0).all().all()
    assert (below.predict(april) == 0.0).all().all()


class Sunk:
    """A booster that says one share of capacity below the real one's."""

    def __init__(self, booster):
        self.booster = booster

    def predict(self, given):
        return self.booster.predict(given) - 1.0


def test_a_missing_input_is_no_obstacle(rows):
    past = rows[rows["target_day"] < date(2025, 2, 1)]
    week_one = past["target_day"] < date(2025, 1, 8)
    holed = past.assign(
        forecast_ghi_mean_7d_w_m2=past["forecast_ghi_mean_7d_w_m2"].mask(week_one),
        solar_mean_7d_mw=past["solar_mean_7d_mw"].mask(week_one),
    )

    learned = train_boosted(holed)

    assert learned.summary["training_slots"] == 31 * 24  # the rows with holes are kept
    assert learned.predict(holed).notna().all().all()


def test_it_can_be_given_fewer_inputs_and_says_how_much_each_one_weighed(rows):
    past = rows[rows["target_day"] < date(2025, 2, 1)]
    few = ("clear_sky_share", "forecast_ghi_w_m2", "forecast_temperature_c")

    learned = train_boosted(past, few)

    weights = {name: learned.summary[f"gain_share/{name}"] for name in few}
    assert learned.inputs == few and sum(weights.values()) == pytest.approx(1.0)
    assert weights["forecast_ghi_w_m2"] > 0.5 > weights["forecast_temperature_c"]
    assert "# p10_mw" in learned.dump() and "forecast_cloud_cover_pct" not in learned.dump()


def test_a_training_repeats_exactly_and_gives_the_file_recorded_here(rows):
    past = rows[rows["target_day"] < date(2025, 2, 1)]

    first, second = train_boosted(past).dump(), train_boosted(past).dump()

    assert first == second
    assert hashlib.sha256(first.encode()).hexdigest() == RECORDED


def test_a_backtest_month_by_month_repeats_to_the_last_file_and_forecast(rows):
    known = rows.assign(fold=[d.replace(day=1) for d in rows["target_day"]])
    asked = known[known["target_day"] >= date(2025, 3, 1)]

    said, trainings = train_by_month(train_boosted, known, asked)
    again, trainings_again = train_by_month(train_boosted, known, asked)

    assert [t["fold"] for t in trainings] == [date(2025, 3, 1), date(2025, 4, 1)]
    assert [t["rows"] for t in trainings] == [(31 + 27) * 48, (31 + 28 + 30) * 48]
    assert len({t["sha256"] for t in trainings}) == 2
    assert [t["sha256"] for t in trainings] == [t["sha256"] for t in trainings_again]
    pd.testing.assert_frame_equal(said, again)
    assert (said["p10_mw"] <= said["predicted_mw"]).all()
    assert (said["predicted_mw"] <= said["p90_mw"]).all()

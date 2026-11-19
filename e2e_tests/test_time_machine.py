"""The time machine's guarantees on the world (README.md): what no single component can prove.

The tests share one world and run in this order. The last two plant raw files into it: each
compares what it needs just before and after its own plant, so neither depends on the other.
"""

import json
from datetime import UTC, date, datetime, timedelta

import pandas as pd
from google.cloud import bigquery
from harness import (
    CODE,
    DECISION_TIMES,
    FIRST_DAY,
    JST,
    LAST_DAY,
    LATE,
    NOT_IN_THE_WINDOW,
    assert_unchanged,
    differences,
)

from jtf.ml.backtest import MEAN_7D, MEAN_7D_SCALED, PERSISTENCE
from jtf.ml.replay import reproduce, run
from jtf.parse.fingerprint import verify

NOW = lambda: datetime.now(UTC)  # noqa: E731
SEEN = ["model", "decision_time"]


def what_if(
    world, run_id: str, first: date = FIRST_DAY, last: date = LAST_DAY, model: str = PERSISTENCE
) -> dict:
    args = {"mode": "what-if", "model": model, "code": CODE, "now": NOW}
    return run(world.warehouse(), run_id=run_id, first_day=first, last_day=last, **args)


def again(world, original: str) -> dict:
    """A reproduce of `original`, which fails unless it matches exactly."""
    args = {"original": original, "code": CODE, "now": NOW}
    return reproduce(world.warehouse(), run_id=f"{original}-again", **args)


def stored(world, run_id: str) -> pd.Series:
    (record,) = world.warehouse().run(run_id).itertuples(index=False)
    return pd.Series(record._asdict())


def test_one_day_goes_from_raw_files_to_a_forecast_and_every_check_passes(world):
    record = what_if(world, "e2e-one-day", date(2026, 11, 17), date(2026, 11, 18))
    mean = what_if(world, "e2e-one-day-mean", date(2026, 11, 17), date(2026, 11, 18), MEAN_7D)
    scaled = what_if(
        world, "e2e-one-day-scaled", date(2026, 11, 17), date(2026, 11, 18), MEAN_7D_SCALED
    )
    world.dbt("run", "--select", "ads_ml_outcome_solar")
    world.dbt("test", "--exclude", "test_type:unit", *NOT_IN_THE_WINDOW)

    features = world.query(f"""
        select decision_time, count(*) as slots, countif(solar_d2_mw is null) as without_d2,
            countif(solar_mean_7d_mw is null) as without_mean_7d,
            countif(capacity_mw is null or clear_sky_mw is null) as without_capacity
        from `{world.table("dws", "dws_solar_features")}`
        group by decision_time order by decision_time
    """)
    assert list(features["decision_time"]) == DECISION_TIMES
    assert set(features["slots"]) == {48} and set(features["without_d2"]) == {0}
    assert set(features["without_mean_7d"]) == {0}
    assert set(features["without_capacity"]) == {0}
    outcomes = world.query(f"""
        select target_day, count(*) as slots, countif(label_mw is not null) as labelled
        from `{world.table("ads", "ads_ml_outcome_solar")}`
        where run_id = 'e2e-one-day'
        group by target_day order by target_day
    """)
    assert list(outcomes["slots"]) == [48, 48]
    assert outcomes["labelled"].iloc[0] == 48  # the 17th, whole in the world's newest capture
    # D−2 read at 09:00 on the 16th was partly dated by rule, and on the 17th all captured
    assert (record["decision_times"], record["exact"], record["approximate"]) == (2, 1, 1)
    assert stored(world, "e2e-one-day")[["exact", "approximate"]].tolist() == [1, 1]
    # The seven days back reach before capture start at both times: read from history, by rule
    assert (mean["decision_times"], mean["exact"], mean["approximate"]) == (2, 0, 2)
    # The world is too young for a week of forecasts, so the scaled mean falls back to the mean:
    # the same predictions from more inputs, the forecast among them
    said = world.query(f"""
        select model, countif(predicted_mw is null) as empty, sum(predicted_mw) as total
        from `{world.table("ads", "ads_ml_prediction_solar")}`
        where run_id in ('e2e-one-day-mean', 'e2e-one-day-scaled')
        group by model order by model
    """)
    assert list(said["empty"]) == [0, 0] and said["total"].nunique() == 1
    assert (
        scaled["decision_times"] == 2 and scaled["inputs_fingerprint"] != mean["inputs_fingerprint"]
    )


def test_a_rebuild_and_a_second_run_of_the_same_days_are_identical(world):
    before = world.fingerprints()
    world.dbt("run", "--full-refresh")  # every model from the ODS again, incremental ones included

    assert_unchanged(before, world.fingerprints(), ["model"])
    what_if(world, "e2e-determinism-1")
    what_if(world, "e2e-determinism-2")
    first, second = stored(world, "e2e-determinism-1"), stored(world, "e2e-determinism-2")
    for side in ("inputs_fingerprint", "outputs_fingerprint"):
        assert first[side] == second[side]


def test_a_reproduce_of_a_what_if_run_matches_it_exactly(world):
    what_if(world, "e2e-reproduce")
    again(world, "e2e-reproduce")  # raises Mismatch unless both fingerprints match
    what_if(world, "e2e-reproduce-mean", model=MEAN_7D)
    again(world, "e2e-reproduce-mean")
    assert stored(world, "e2e-reproduce-mean-again")["model"] == MEAN_7D

    original, reproduced = stored(world, "e2e-reproduce"), stored(world, "e2e-reproduce-again")
    assert (reproduced["mode"], reproduced["reproduces"]) == ("reproduce", "e2e-reproduce")
    for side in ("inputs_fingerprint", "outputs_fingerprint"):
        assert reproduced[side] == original[side]


# An hour after the last decision time: a daily capture, but known after every T of the window
POISONED_AT = datetime(2026, 11, 18, 1, 0, tzinfo=UTC)
ABSURD = "999999"


def absurd_actuals(body: bytes) -> bytes:
    """Every solar, curtailed and wind figure of a TEPCO actuals file at 999,999 MW."""
    lines = body.decode("cp932").split("\n")
    for i, line in enumerate(lines):
        cells = line.split(",")
        if cells[0][:4].isdigit() and len(cells) > 13:  # a slot's row, not the header
            cells[11:14] = [ABSURD if c.strip() else c for c in cells[11:14]]
            lines[i] = ",".join(cells)
    return "\n".join(lines).encode("cp932")


def absurd_weather(body: bytes) -> bytes:
    """Every value of an Open-Meteo response at 999,999, its times as they were."""
    points = json.loads(body)
    for point in points:
        hourly = point["hourly"]
        for name in hourly.keys() - {"time"}:
            hourly[name] = [None if v is None else int(ABSURD) for v in hourly[name]]
    return json.dumps(points).encode()


def test_rows_known_after_t_with_absurd_values_change_nothing_at_t(world, tmp_path):
    before = world.seen()
    what_if(world, "e2e-poison")
    what_if(world, "e2e-poison-mean", model=MEAN_7D)
    for source, name, change in [
        ("tepco_actuals", "_eria_jukyu_202611_03.csv", absurd_actuals),
        ("openmeteo_live", "_forecast.json", absurd_weather),
        ("openmeteo_previous_runs", ".json", absurd_weather),  # values it has, so corrections
    ]:
        world.plant(
            tmp_path,
            world.newest(source, name),
            fetched_at=POISONED_AT,
            load_type="daily",
            change=change,
        )
    world.parse(tmp_path, batch_id="e2e-poison")
    world.dbt("run")

    # The poison is in DWD, so a pass means as_of kept it out, not that it never arrived
    poisoned = world.query(
        " union all ".join(
            f"select '{model}' as model, countif(known_at = @at) as planted "
            f"from `{world.table('dwd', model)}`"
            for model in ("dwd_tepco_actuals", "dwd_weather_live", "dwd_weather_previous_runs")
        ),
        [bigquery.ScalarQueryParameter("at", "TIMESTAMP", POISONED_AT)],
    )
    assert (poisoned["planted"] > 0).all(), poisoned
    assert_unchanged(before, world.seen(), SEEN)
    again(world, "e2e-poison")
    again(world, "e2e-poison-mean")  # the seven days it reads were all poisoned after T


# The time of a late history load, after the window: "now" in the world's time
LOADED_LATE_AT = datetime(2026, 11, 19, 3, 0, tzinfo=UTC)
# Times before the window, when only rule-dated history existed, and the window's own
PAST = [pd.Timestamp(day, tz=JST) for day in ("2026-10-20 09:00", "2026-11-05 09:00")]
PAST += DECISION_TIMES
CORRECTED_SLOT, CORRECTION_MW = "2026/11/13,12:00,", 1000


def corrected(body: bytes) -> bytes:
    """A TEPCO actuals file with the solar figure of one slot 1,000 MW higher: a correction."""
    lines = body.decode("cp932").split("\n")
    (i,) = [i for i, line in enumerate(lines) if line.startswith(CORRECTED_SLOT)]
    cells = lines[i].split(",")
    cells[11] = str(int(cells[11]) + CORRECTION_MW)
    lines[i] = ",".join(cells)
    return "\n".join(lines).encode("cp932")


def versions_read(world, times: list[pd.Timestamp]) -> pd.DataFrame:
    """The actuals versions each of `times` reads, through the as_of predicate."""
    return world.query(
        f"""
        select t as as_of, v.slot_start, v.measure, v.value_mw, v.known_at, v.basis
        from `{world.table("dwd", "dwd_tepco_actuals")}` as v, unnest(@times) as t
        where v.known_at <= t and (v.known_until is null or t < v.known_until)
        """,
        [bigquery.ArrayQueryParameter("times", "TIMESTAMP", [t.to_pydatetime() for t in times])],
    )


def test_a_late_history_load_changes_nothing_a_past_decision_time_saw(world, tmp_path):
    before, read_before = world.seen(), versions_read(world, PAST)
    what_if(world, "e2e-backfill")
    what_if(world, "e2e-backfill-mean", model=MEAN_7D)
    (october,) = [f for f in LATE.rglob("*.csv")]  # a month the world has no file of
    november = world.newest("tepco_actuals", "_eria_jukyu_202611_03.csv")
    world.plant(tmp_path, october, fetched_at=LOADED_LATE_AT, load_type="initial")
    world.plant(
        tmp_path, november, fetched_at=LOADED_LATE_AT, load_type="initial", change=corrected
    )
    world.parse(tmp_path, batch_id="e2e-backfill")
    world.dbt("run")

    # The late load only appended: every file loaded before is as loaded, by its fingerprint
    ods = verify(world.ods(), "tepco_actuals", everything=True)
    assert ods.ok, ods.problems

    assert_unchanged(before, world.seen(), SEEN)
    added = differences(read_before, versions_read(world, PAST), ["as_of", "slot_start"])
    assert set(added["_merge"]) == {"right_only"}, "a version read before is gone"
    # New rows only by rule, and only for slots before the actuals' capture start
    capture_start = world.query(f"""
        select min(known_at) as t from `{world.table("dwd", "dwd_source_captures")}`
        where source = 'tepco_actuals'
    """)["t"].iloc[0]
    assert len(added) > 0  # October, by the rule, at every time after it
    assert set(added["basis"]) == {"rule"}
    assert (added["slot_start"] + timedelta(minutes=90) < capture_start).all()
    # The correction is a version known only from the late load, which no past time reads
    slot = pd.Timestamp("2026-11-13 12:00", tz=JST)
    versions = world.query(
        f"""
        select value_mw, known_at, basis from `{world.table("dwd", "dwd_tepco_actuals")}`
        where measure = 'solar' and slot_start = @slot order by known_at
        """,
        [bigquery.ScalarQueryParameter("slot", "TIMESTAMP", slot.to_pydatetime())],
    )
    newest = versions.iloc[-1]
    assert (newest["known_at"], newest["basis"]) == (LOADED_LATE_AT, "captured")
    assert newest["value_mw"] == versions["value_mw"].iloc[0] + CORRECTION_MW
    again(world, "e2e-backfill")
    again(world, "e2e-backfill-mean")  # the corrected slot is one of the seven days it reads

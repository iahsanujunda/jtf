"""Every parser against fixtures cut from real raw files (`tepco_actuals` has its own module)."""

import io
import json
import zipfile
from datetime import UTC, datetime

import pandas as pd
import pytest

from conftest import FIXTURES
from jtf.ingest.sources import SOURCES
from jtf.parse import PARSERS, parsers_for
from jtf.parse.base import RawFile, Refused, read_raw
from jtf.parse.load import MANIFEST, parse_day
from jtf.storage import LocalStore

DAY = "2026-11-15"
STORE = LocalStore(FIXTURES)


def fixture(source, name):
    (path,) = [p for p in STORE.list(f"{source}/{DAY}/") if p.endswith(f"_{name}")]
    return read_raw(STORE, path)


def edited(raw, edit):
    return RawFile(raw.path, raw.meta, edit(raw.body))


def test_every_source_has_a_parser_and_every_table_one_schema():
    assert {p.source for p in PARSERS.values()} == set(SOURCES)
    for parser in PARSERS.values():
        names = [c.name for c in parser.schema]
        assert len(names) == len(set(names)), parser.table


# ---------------------------------------------------------------- TEPCO area files


def test_area_files_parse_by_position_whatever_their_header_says():
    forecast = PARSERS["tepco_area_forecast"].parse(
        fixture("tepco_area_forecast", "AREA_YOSOKU.csv")
    )
    jiseki = PARSERS["tepco_area_actuals_today"].parse(
        fixture("tepco_area_actuals_today", "AREA_JISEKI.csv")
    )
    assert len(forecast) == len(jiseki) == 48
    assert list(forecast.columns) == list(jiseki.columns)
    assert (forecast["slot_from"].iloc[0], jiseki["slot_from"].iloc[0]) == ("0000", "0:00")
    assert jiseki["source_time"].iloc[0] == pd.Timestamp("2026-11-15 12:05:01", tz="Asia/Tokyo")
    assert PARSERS["tepco_area_forecast"].check(forecast) == []


def test_a_next_day_file_outside_the_evening_is_empty_not_refused():
    raw = fixture("tepco_area_forecast_next_day", "AREA_ONCE_YOSOKU.csv")
    assert PARSERS["tepco_area_forecast_next_day"].parse(raw).empty


def test_an_area_file_with_another_column_count_is_refused():
    raw = fixture("tepco_area_forecast", "AREA_YOSOKU.csv")
    text = raw.text().replace("エリア風力・太陽光発電量[30分kWh]", "風力[30分kWh],太陽光[30分kWh]")
    with pytest.raises(Refused, match="header has 8 columns"):
        PARSERS["tepco_area_forecast"].parse(edited(raw, lambda _: text.encode()))


# ---------------------------------------------------------------- JEPX


def test_jepx_volumes_are_whole_numbers_and_prices_decimals():
    rows = PARSERS["jepx_spot"].parse(fixture("jepx_spot", "spot_summary_2026.csv"))
    assert len(rows) == 96 and rows["delivery_date"].iloc[0] == "2026/04/01"
    assert str(rows["contracted_kwh"].dtype) == "Int64"
    assert str(rows["price_tokyo_yen_kwh"].dtype) == "Float64"
    assert rows["source_time"].isna().all()  # JEPX sends no Last-Modified
    assert PARSERS["jepx_spot"].check(rows) == []


def test_a_jepx_column_without_a_name_is_refused():
    raw = fixture("jepx_spot", "spot_summary_2026.csv")
    text = raw.text().replace("約定総量(kWh)", "約定総量(MWh)", 1)
    with pytest.raises(Refused, match="no ASCII name"):
        PARSERS["jepx_spot"].parse(edited(raw, lambda _: text.encode()))


# ---------------------------------------------------------------- yearly actuals


def test_yearly_actuals_join_two_header_rows_and_drop_thousands_separators():
    rows = PARSERS["tepco_actuals_yearly"].parse(fixture("tepco_actuals", "area-2016.csv"))
    first = rows.iloc[0]
    assert (first["date"], first["time"], first["area_demand"]) == ("2016/4/1", "0:00", 2555)
    assert len(rows) == 51 and "thermal" in rows.columns
    # Yearly files hold whole days; this fixture stops three hours into its third day.
    check = PARSERS["tepco_actuals_yearly"].check
    assert check(rows) == ["a day without 24 hours"]
    assert check(rows.iloc[:48]) == []


def test_tepco_actuals_runs_both_layouts_each_into_its_own_table(warehouse):
    for parser in parsers_for("tepco_actuals"):
        parse_day(parser, STORE, warehouse, DAY, batch_id="b", now=datetime.now(UTC))
    assert len(warehouse.rows("tepco_actuals")) == 202
    assert len(warehouse.rows("tepco_actuals_yearly")) == 51
    assert warehouse.rows(MANIFEST)["ods_table"].value_counts().to_dict() == {
        "tepco_actuals": 2,
        "tepco_actuals_yearly": 1,
    }


# ---------------------------------------------------------------- demand outlook


def test_todays_demand_outlook_splits_into_three_shapes():
    raw = fixture("tepco_demand_outlook", "juyo-d1-j.csv")
    peaks = PARSERS["tepco_demand_outlook_peaks"].parse(raw)
    hourly = PARSERS["tepco_demand_outlook_hourly"].parse(raw)
    five = PARSERS["tepco_demand_outlook_5min"].parse(raw)
    assert (len(peaks), len(hourly), len(five)) == (9, 24, 288)
    assert peaks.groupby("day").size().to_dict() == {"today": 5, "tomorrow": 4}
    assert peaks.set_index(["day", "figure"]).loc[("today", "peak_supply"), "value"] == 3411
    assert peaks.loc[peaks["day"] == "tomorrow", "value"].isna().all()  # empty at 12:24
    assert hourly["source_time"].iloc[0] == pd.Timestamp("2026-11-15 12:15", tz="Asia/Tokyo")
    assert hourly["member"].isna().all()
    assert five["solar_man_kw"].isna().sum() == 140  # rows after 12:15 haven't happened


def test_an_archive_zip_gives_rows_per_daily_file_with_its_own_update_time():
    raw = fixture("tepco_demand_outlook", "202611_power_usage.zip")
    hourly = PARSERS["tepco_demand_outlook_hourly"].parse(raw)
    assert hourly.groupby("member").size().to_dict() == {
        "20261101_power_usage.csv": 24,
        "20261102_power_usage.csv": 24,
    }
    updated = hourly.groupby("member")["source_time"].first()
    assert updated.iloc[0] == pd.Timestamp("2026-11-01 23:55", tz="Asia/Tokyo")
    assert PARSERS["tepco_demand_outlook_hourly"].check(hourly) == []


def test_a_zip_holding_anything_but_daily_files_is_refused():
    raw = fixture("tepco_demand_outlook", "202611_power_usage.zip")
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        z.writestr("readme.txt", "hello")
    with pytest.raises(Refused, match="unexpected member"):
        PARSERS["tepco_demand_outlook_5min"].parse(edited(raw, lambda _: buffer.getvalue()))


# ---------------------------------------------------------------- Open-Meteo


def test_previous_runs_unpack_model_and_lead_from_column_names():
    raw = fixture("openmeteo_previous_runs", "previous_runs_2026-10-01_2026-10-31.json")
    rows = PARSERS["openmeteo_previous_runs"].parse(raw)
    assert set(rows["model"]) == {"jma_msm", "jma_gsm", "ecmwf_ifs025"}
    assert set(rows["lead_days"]) == {0, 1, 2}
    assert len(rows) == 9 * 3 * 3 * 6  # points x models x leads x hours
    assert rows.loc[rows["model"] == "jma_gsm", "shortwave_radiation"].isna().all()
    assert PARSERS["openmeteo_previous_runs"].check(rows) == []


def test_a_single_run_keeps_its_run_time_and_drops_hours_with_no_values():
    raw = fixture("openmeteo_single_runs", "jma_msm_2026-09-12T21Z.json")
    rows = PARSERS["openmeteo_single_runs"].parse(raw)
    assert set(rows["model"]) == {"jma_msm"}  # one model: no suffix in the column names
    assert (rows["run_time"] == pd.Timestamp("2026-09-12 21:00", tz="UTC")).all()
    assert "lead_days" not in rows.columns


@pytest.mark.parametrize(
    ("edit", "reason"),
    [
        (lambda e: e["hourly_units"].update(wind_speed_10m_jma_msm="m/s"), "km/h"),
        (lambda e: e.update(timezone="Asia/Tokyo"), "time zone"),
    ],
)
def test_a_changed_unit_or_time_zone_is_refused(edit, reason):
    raw = fixture("openmeteo_live", "forecast.json")

    def change(body):
        data = json.loads(body)
        edit(data[0])
        return json.dumps(data).encode()

    with pytest.raises(Refused, match=reason):
        PARSERS["openmeteo_live"].parse(edited(raw, change))


def test_a_recorded_gap_gives_a_manifest_row_and_no_rows(tmp_path, warehouse):
    store = LocalStore(tmp_path)
    path = f"openmeteo_single_runs/{DAY}/20261116T051500Z_jma_msm_2026-05-01T18Z.json"
    store.put_new(path, b'{"error":true,"reason":"The requested model run is not available."}', "x")
    meta = {
        "filename": "jma_msm_2026-05-01T18Z.json",
        "extraction_date": DAY,
        "gap": True,
        "fetched_at": "2026-11-16T05:15:00+00:00",
        "load_type": "initial",
        "http_status": 400,
        "size_bytes": 66,
        "sha256": "-",
        "params": {"models": "jma_msm"},
    }
    store.put_new(path + ".meta.json", json.dumps(meta).encode(), "application/json")

    result = parse_day(
        PARSERS["openmeteo_single_runs"], store, warehouse, DAY, batch_id="b", now=datetime.now(UTC)
    )

    assert result.gaps == [path] and result.ok
    assert warehouse.rows("openmeteo_single_runs").empty
    assert list(warehouse.rows(MANIFEST)["status"]) == ["gap"]

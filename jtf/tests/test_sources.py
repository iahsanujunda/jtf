from datetime import datetime

import pytest

from jtf.ingest.sources import JST, PATIENT, SOURCES, fiscal_year, get_source

NOW = datetime(2026, 11, 15, 8, 45, tzinfo=JST)


def filenames(name, when=NOW):
    return [i.filename for i in get_source(name).items(when)]


def test_every_source_fetches_files_stored_under_its_own_name():
    for name, source in SOURCES.items():
        items = source.items(NOW)
        assert items, name
        assert {i.source for i in items} == {name}


def test_one_file_per_tepco_same_day_and_next_day_source():
    assert filenames("tepco_area_forecast") == ["AREA_YOSOKU.csv"]
    assert filenames("tepco_area_bg_plan") == ["AREA_BGKEI.csv"]
    assert filenames("tepco_area_actuals_today") == ["AREA_JISEKI.csv"]
    assert filenames("tepco_demand_outlook") == ["juyo-d1-j.csv"]
    assert filenames("tepco_area_forecast_next_day") == ["AREA_ONCE_YOSOKU.csv"]
    assert filenames("tepco_area_bg_plan_next_day") == ["AREA_ONCE_BGKEI.csv"]


def test_actuals_fetch_current_month_and_previous_month_early_on():
    assert filenames("tepco_actuals") == ["eria_jukyu_202611_03.csv"]
    early = datetime(2027, 1, 3, 8, 45, tzinfo=JST)
    assert filenames("tepco_actuals", early) == [
        "eria_jukyu_202612_03.csv",
        "eria_jukyu_202701_03.csv",
    ]


@pytest.mark.parametrize(
    ("when", "fy"),
    [(datetime(2026, 4, 1, tzinfo=JST), 2026), (datetime(2027, 3, 31, tzinfo=JST), 2026)],
)
def test_jepx_fiscal_year_starts_in_april(when, fy):
    assert fiscal_year(when) == fy
    (item,) = get_source("jepx_spot").items(when)
    assert item.method == "POST"
    assert item.form == {"dir": "spot_summary", "file": f"spot_summary_{fy}.csv"}


def test_open_meteo_is_patient_and_slugs_use_hyphens():
    source = get_source("openmeteo_live")
    assert source.retry is PATIENT
    assert source.retry.attempts == 5
    assert source.slug == "openmeteo-live"


def test_unknown_source_is_rejected():
    with pytest.raises(ValueError):
        get_source("tepco_sameday")

from datetime import UTC, datetime

import pytest

from jtf.ingest.sources import (
    JST,
    PATIENT,
    SINGLE_RUNS,
    SOURCES,
    fiscal_year,
    get_source,
)

NOW = datetime(2026, 11, 15, 8, 45, tzinfo=JST)


def filenames(name, mode="daily", when=NOW):
    return [i.filename for i in get_source(name).items_for(mode)(when)]


def test_every_item_is_stored_under_its_own_source():
    for name, source in SOURCES.items():
        for mode in source.modes:
            items = source.items_for(mode)(NOW)
            assert items, (name, mode)
            assert {i.source for i in items} == {name}, (name, mode)


def test_one_file_per_tepco_same_day_and_next_day_source():
    assert filenames("tepco_area_forecast") == ["AREA_YOSOKU.csv"]
    assert filenames("tepco_area_bg_plan") == ["AREA_BGKEI.csv"]
    assert filenames("tepco_area_actuals_today") == ["AREA_JISEKI.csv"]
    assert filenames("tepco_demand_outlook") == ["juyo-d1-j.csv"]
    assert filenames("tepco_area_forecast_next_day") == ["AREA_ONCE_YOSOKU.csv"]
    assert filenames("tepco_area_bg_plan_next_day") == ["AREA_ONCE_BGKEI.csv"]


def test_actuals_capture_the_previous_and_current_month_every_day():
    assert filenames("tepco_actuals") == ["eria_jukyu_202610_03.csv", "eria_jukyu_202611_03.csv"]
    january = datetime(2027, 1, 20, 8, 45, tzinfo=JST)
    assert filenames("tepco_actuals", when=january) == [
        "eria_jukyu_202612_03.csv",
        "eria_jukyu_202701_03.csv",
    ]


def test_actuals_history_is_the_yearly_files_and_every_monthly_file():
    history = filenames("tepco_actuals", "initial")
    assert history[:8] == [f"area-{fy}.csv" for fy in range(2016, 2024)]
    assert history[8] == "eria_jukyu_202402_03.csv"
    assert history[-1] == "eria_jukyu_202611_03.csv"
    assert len(history) == 8 + 34


def test_actuals_recapture_checks_every_monthly_file():
    recapture = filenames("tepco_actuals", "recapture")
    assert recapture[0] == "eria_jukyu_202402_03.csv"
    assert len(recapture) == 34


@pytest.mark.parametrize(
    ("when", "fy"),
    [(datetime(2026, 4, 1, tzinfo=JST), 2026), (datetime(2027, 3, 31, tzinfo=JST), 2026)],
)
def test_jepx_fiscal_year_starts_in_april(when, fy):
    assert fiscal_year(when) == fy
    (item,) = get_source("jepx_spot").items(when)
    assert item.method == "POST"
    assert item.form == {"dir": "spot_summary", "file": f"spot_summary_{fy}.csv"}


def test_jepx_history_is_every_past_fiscal_year_from_2016():
    assert filenames("jepx_spot", "initial") == [
        f"spot_summary_{fy}.csv" for fy in range(2016, 2026)
    ]


def test_previous_runs_history_is_one_request_per_month_up_to_yesterday():
    items = get_source("openmeteo_previous_runs").history(NOW)
    assert items[0].filename == "previous_runs_2024-01-01_2024-01-31.json"
    assert items[-1].filename == "previous_runs_2026-11-01_2026-11-14.json"
    assert len(items) == 35
    hourly = items[0].params["hourly"].split(",")
    assert "shortwave_radiation_previous_day2" in hourly and len(hourly) == 18
    assert items[0].params["models"] == "jma_msm,jma_gsm,ecmwf_ifs025"


def test_single_runs_history_covers_each_chosen_run_from_the_archive_start():
    items = get_source("openmeteo_single_runs").history(datetime(2026, 4, 5, 3, tzinfo=UTC))
    assert len(items) == 2 * len(SINGLE_RUNS)  # 2 and 3 April
    assert items[0].filename == "jma_msm_2026-04-02T18Z.json"
    assert items[0].params["run"] == "2026-04-02T18:00"
    assert items[0].params["models"] == "jma_msm"


def test_open_meteo_sources_are_patient_and_budgeted():
    assert get_source("openmeteo_live").retry is PATIENT
    assert get_source("openmeteo_single_runs").call_budget == 8000
    assert get_source("openmeteo_previous_runs").slug == "openmeteo-previous-runs"


def test_history_only_sources_have_no_daily_items():
    assert get_source("openmeteo_single_runs").modes == ["initial"]
    assert get_source("tepco_actuals").modes == ["daily", "initial", "recapture"]


def test_unknown_source_is_rejected():
    with pytest.raises(ValueError):
        get_source("tepco_sameday")

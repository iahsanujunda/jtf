import csv
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from conftest import FIXTURES
from jtf.ingest.sources import (
    JST,
    PATIENT,
    SINGLE_RUNS,
    SOURCES,
    WEATHER_POINTS,
    fiscal_year,
    fit_links,
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


def test_demand_outlook_takes_todays_file_and_the_last_two_archive_months():
    assert filenames("tepco_demand_outlook") == [
        "juyo-d1-j.csv",
        "202610_power_usage.zip",
        "202611_power_usage.zip",
    ]
    # On the 1st, this month's zip doesn't exist yet: yesterday's month is the newest.
    first = datetime(2026, 12, 1, 8, 45, tzinfo=JST)
    assert filenames("tepco_demand_outlook", when=first)[1:] == [
        "202610_power_usage.zip",
        "202611_power_usage.zip",
    ]


def test_demand_outlook_history_is_every_archive_month_since_april_2022():
    months = filenames("tepco_demand_outlook", "initial")
    assert months[0] == "202204_power_usage.zip"
    assert months[-1] == "202611_power_usage.zip"
    assert len(months) == 56
    assert filenames("tepco_demand_outlook", "recapture") == months


# ---------------------------------------------------------------- FIT/FIP solar capacity

FIT_PAGE_FIXTURE = next(
    p for p in (FIXTURES / "meti_fit_capacity").rglob("*Z_PublicInfoSummary.html")
)


def test_fit_links_find_every_xlsx_release_on_the_page():
    items = fit_links(FIT_PAGE_FIXTURE.read_bytes())
    names = [i.filename for i in items]

    assert len(names) == 66 and len(set(names)) == 66  # 33 quarters of tables A and B
    assert names[:2] == ["A_pref202603.xlsx", "B_city202603.xlsx"]
    assert not any(n[6:12] in {"201709", "201712", "201803"} for n in names)  # the .xls ones
    assert items[0].params == {"file": "015hA000000lPEH"}
    assert {i.url for i in items} == {"https://www.fit-portal.go.jp/servlet/servlet.FileDownload"}


def test_fit_links_fail_loudly_when_the_page_links_nothing():
    with pytest.raises(ValueError, match="links no table A or B release"):
        fit_links(b"<html><body>maintenance</body></html>")


def test_fit_daily_capture_is_the_page_and_each_linked_release_watched():
    (page,) = get_source("meti_fit_capacity").items(NOW)
    assert page.filename == "PublicInfoSummary.html" and page.expand is not None
    linked = page.expand(FIT_PAGE_FIXTURE.read_bytes())
    assert len(linked) == 66 and all(i.watched for i in linked)


def test_fit_history_adds_every_archived_copy_of_the_page():
    cdx, page = get_source("meti_fit_capacity").history(NOW)
    assert cdx.filename == "wayback_cdx.json" and page.filename == "PublicInfoSummary.html"
    answer = json.dumps([["timestamp"], ["20181018201547"], ["20240302210103"]]).encode()

    snapshots = cdx.expand(answer)

    assert [s.filename for s in snapshots] == [
        "wayback_20181018201547_PublicInfoSummary.html",
        "wayback_20240302210103_PublicInfoSummary.html",
    ]
    assert snapshots[1].url == (
        "https://web.archive.org/web/20240302210103id_/https://www.fit-portal.go.jp/PublicInfoSummary"
    )


def test_the_warehouse_seed_holds_the_same_weather_points():
    # dbt computes the sun at these points; the capture fetches weather at them. One definition.
    seed = Path(__file__).parents[2] / "warehouse/dbt/seeds/dim_weather_points.csv"
    with seed.open() as f:
        rows = [(r["point"], float(r["lat"]), float(r["lon"])) for r in csv.DictReader(f)]
    assert rows == list(WEATHER_POINTS)

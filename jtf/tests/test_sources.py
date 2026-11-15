from datetime import datetime

import pytest

from jtf.ingest.sources import JST, fiscal_year, items_for_group


def names(group, when):
    return [i.filename for i in items_for_group(group, when)]


def test_morning_fetches_current_month_and_same_day_files():
    got = names("morning", datetime(2026, 11, 15, 8, 45, tzinfo=JST))
    assert got == [
        "eria_jukyu_202611_03.csv",
        "AREA_YOSOKU.csv",
        "AREA_BGKEI.csv",
        "AREA_JISEKI.csv",
        "juyo-d1-j.csv",
        "forecast_jma.json",
    ]


def test_morning_adds_previous_month_early_in_the_month():
    got = names("morning", datetime(2027, 1, 3, 8, 45, tzinfo=JST))
    assert got[:2] == ["eria_jukyu_202612_03.csv", "eria_jukyu_202701_03.csv"]


@pytest.mark.parametrize(
    ("when", "fy"),
    [(datetime(2026, 4, 1, tzinfo=JST), 2026), (datetime(2027, 3, 31, tzinfo=JST), 2026)],
)
def test_jepx_fiscal_year_starts_in_april(when, fy):
    assert fiscal_year(when) == fy
    (item,) = items_for_group("jepx", when)
    assert item.method == "POST"
    assert item.form == {"dir": "spot_summary", "file": f"spot_summary_{fy}.csv"}


def test_evening_fetches_next_day_forecast():
    assert names("evening", datetime(2026, 11, 15, 19, tzinfo=JST)) == [
        "AREA_ONCE_YOSOKU.csv",
        "AREA_ONCE_BGKEI.csv",
    ]


def test_unknown_group_is_rejected():
    with pytest.raises(ValueError):
        items_for_group("noon", datetime(2026, 11, 15, tzinfo=JST))

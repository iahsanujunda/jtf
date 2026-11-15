from datetime import date

import pandas as pd
import pytest

from conftest import FIXTURES
from jtf.parse.base import Refused, read_raw
from jtf.parse.tepco_actuals import MEASURES, PARSER, check, parse
from jtf.storage import LocalStore

DAY = "tepco_actuals/2026-11-15"
CP932 = f"{DAY}/20261115T020416Z_eria_jukyu_202611_03.csv"  # a daily capture
UTF8 = f"{DAY}/20261115T045300Z_eria_jukyu_202402_03.csv"  # from the history load


def raw(path):
    return read_raw(LocalStore(FIXTURES), path)


def edited(path, edit):
    """The fixture with its text changed, as a UTF-8 file."""
    original = raw(path)
    return type(original)(original.path, original.meta, edit(original.text()).encode())


def test_both_encodings_parse_to_the_same_columns():
    november, february = parse(raw(CP932)), parse(raw(UTF8))
    assert list(november.columns) == list(february.columns)
    assert len(november) == len(february) == 101  # two full days and five slots
    first = november.iloc[0]
    assert (first["date"], first["time"], first["area_demand"]) == ("2026/11/1", "0:00", 21530)
    assert february.iloc[0]["area_demand"] == 28755
    assert all(str(november[m].dtype) == "Int64" for m in MEASURES)


def test_lineage_comes_from_the_sidecar():
    rows = parse(raw(CP932))
    first = rows.iloc[0]
    assert first["source_file"] == CP932
    assert first["extraction_date"] == date(2026, 11, 15)
    assert first["load_type"] == "daily"
    assert first["fetched_at"] == pd.Timestamp("2026-11-15T02:04:16.643393Z")
    assert first["source_time"] == pd.Timestamp("2026-11-15T01:40:04Z")  # Last-Modified
    assert first["parser_version"] == PARSER.version
    assert parse(raw(UTF8)).iloc[0]["load_type"] == "initial"


def test_real_files_pass_their_checks():
    assert check(parse(raw(CP932))) == []
    assert check(parse(raw(UTF8))) == []


@pytest.mark.parametrize(
    ("edit", "reason"),
    [
        (lambda t: t.replace("単位[MW平均]", "単位[万kW]", 1), "unit row"),
        (lambda t: t.replace("その他", "その他(新)", 1), "no ASCII name"),
        (lambda t: t.replace("2026/11/1,0:00,21530", "2026/11/1,0:00,21530.5", 1), "whole number"),
    ],
)
def test_a_changed_shape_is_refused(edit, reason):
    with pytest.raises(Refused, match=reason):
        parse(edited(CP932, edit))


def test_a_dropped_column_becomes_nulls():
    def drop_battery(text):
        lines = text.splitlines()
        position = lines[1].split(",").index("蓄電池")
        rows = [
            ",".join(v for i, v in enumerate(line.split(",")) if i != position)
            for line in lines[1:]
        ]
        return "\n".join([lines[0], *rows])

    rows = parse(edited(CP932, drop_battery))
    assert rows["battery"].isna().all()
    assert rows["solar"].notna().all()


def test_check_flags_duplicate_slots_and_short_days():
    rows = parse(raw(CP932))
    assert "1 duplicate slots" in check(pd.concat([rows, rows.iloc[[0]]]))
    assert "a complete day without 48 slots" in check(rows.drop(index=3))

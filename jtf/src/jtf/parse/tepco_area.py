"""TEPCO's area files: today's and tomorrow's forecast and plans, and today's actuals so far.

Five sources share one layout, `AREA_*.csv`. Row 1 names the fields of row 2: the file's update
date and time and the day it is for. Row 3 is the header, then one row per 30-minute slot,
`時間コマ` 1–48, with three values in kWh per slot. The headers are spelled differently between
files (`AREA_JISEKI` has no units and a full-width `＿`), so columns are taken by position. The
next-day files hold only the header outside the evening. `AREA_JISEKI` fills the slots after its
update time with zeros, which DWD drops.
"""

from __future__ import annotations

import csv

import pandas as pd

from jtf.parse.base import Column, Parser, RawFile, Refused, whole_numbers

VERSION = "1"
FILES = {
    "tepco_area_forecast_next_day": "AREA_ONCE_YOSOKU.csv",
    "tepco_area_bg_plan_next_day": "AREA_ONCE_BGKEI.csv",
    "tepco_area_forecast": "AREA_YOSOKU.csv",
    "tepco_area_bg_plan": "AREA_BGKEI.csv",
    "tepco_area_actuals_today": "AREA_JISEKI.csv",
}
COLUMNS = [
    "date",
    "slot_code",
    "slot_from",
    "slot_to",
    "demand_kwh",
    "generation_kwh",
    "wind_solar_kwh",
]
VALUES = ["slot_code", "demand_kwh", "generation_kwh", "wind_solar_kwh"]


def parse(raw: RawFile) -> pd.DataFrame:
    lines = raw.text().splitlines()
    if (
        len(lines) < 3
        or not lines[0].startswith("ファイル更新日")
        or not lines[2].startswith("日付")
    ):
        raise Refused("not the area-file layout")
    if len(lines[2].split(",")) != len(COLUMNS):
        raise Refused(f"the header has {len(lines[2].split(','))} columns, expected {len(COLUMNS)}")
    updated_date, updated_time, target_date = lines[1].split(",")[:3]
    rows = list(csv.reader(line for line in lines[3:] if line.strip()))
    if any(len(row) != len(COLUMNS) for row in rows):
        raise Refused("a slot row doesn't have the header's columns")
    df = pd.DataFrame(rows, columns=COLUMNS)
    df.insert(0, "row_number", range(1, len(df) + 1))
    df.insert(1, "target_date", target_date)
    df = whole_numbers(df, VALUES)
    written = pd.Timestamp(f"{updated_date} {updated_time}", tz="Asia/Tokyo")
    return df.assign(**raw.lineage(VERSION, source_time=written))


def check(df: pd.DataFrame) -> list[str]:
    if df.empty:
        return []
    issues = []
    if len(df) != 48 or set(df["slot_code"]) != set(range(1, 49)):
        issues.append(f"{len(df)} slots, expected slot codes 1-48 once each")
    if (df["date"] != df["target_date"]).any():
        issues.append("a slot's date differs from the file's target day")
    return issues


def _parser(source: str, filename: str) -> Parser:
    return Parser(
        source=source,
        version=VERSION,
        columns=(
            Column("row_number", "INTEGER", required=True),
            Column("target_date", "STRING", required=True),
            Column("date", "STRING", required=True),
            Column("slot_code", "INTEGER", required=True),
            Column("slot_from", "STRING", required=True),
            Column("slot_to", "STRING", required=True),
            Column("demand_kwh", "INTEGER"),
            Column("generation_kwh", "INTEGER"),
            Column("wind_solar_kwh", "INTEGER"),
        ),
        parse=parse,
        check=check,
        reads=lambda name: name == filename,
    )


PARSERS = [_parser(source, filename) for source, filename in FILES.items()]

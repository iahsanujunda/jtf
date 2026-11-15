"""TEPCO's monthly area actuals, `eria_jukyu_YYYYMM_03.csv`: the solar target.

Row 1 is a unit row, `単位[MW平均]` (MW averaged over the slot); row 2 is the header; then one row
per 30-minute slot, labelled by its start (`2026/11/1`, `0:00`). Every value seen so far is a
whole number of MW, so a decimal is refused as a likely unit change. The yearly `area-YYYY.csv`
files under the same prefix are another layout (万kWh, hourly, a two-row header) with a parser and
table of their own.
"""

from __future__ import annotations

import io

import pandas as pd

from jtf.parse.base import Column, Parser, RawFile, Refused, whole_numbers

VERSION = "1"
UNIT_ROW = "単位[MW平均]"
COLUMNS = {
    "DATE": "date",
    "TIME": "time",
    "エリア需要": "area_demand",
    "原子力": "nuclear",
    "火力(LNG)": "thermal_lng",
    "火力(石炭)": "thermal_coal",
    "火力(石油)": "thermal_oil",
    "火力(その他)": "thermal_other",
    "水力": "hydro",
    "地熱": "geothermal",
    "バイオマス": "biomass",
    "太陽光発電実績": "solar",
    "太陽光出力制御量": "solar_curtailed",
    "風力発電実績": "wind",
    "風力出力制御量": "wind_curtailed",
    "揚水": "pumped_storage",
    "蓄電池": "battery",
    "連系線": "interconnectors",
    "その他": "other",
    "合計": "total",
}
MEASURES = [name for name in COLUMNS.values() if name not in ("date", "time")]


def parse(raw: RawFile) -> pd.DataFrame:
    lines = raw.text().splitlines()
    unit = lines[0].split(",")[0] if lines else ""
    if unit != UNIT_ROW:
        raise Refused(f"unit row is {unit!r}, expected {UNIT_ROW!r}")
    df = pd.read_csv(io.StringIO("\n".join(lines[1:])), dtype=str)
    unknown = sorted(set(df.columns) - set(COLUMNS))
    if unknown:
        raise Refused(f"no ASCII name for columns {unknown}")
    # A column TEPCO drops becomes nulls; one it adds is refused above.
    df = df.rename(columns=COLUMNS).reindex(columns=list(COLUMNS.values()))
    df.insert(0, "row_number", range(1, len(df) + 1))
    df = whole_numbers(df.dropna(subset=["area_demand"]), MEASURES)
    return df.assign(**raw.lineage(VERSION)).reset_index(drop=True)


def check(df: pd.DataFrame) -> list[str]:
    if df.empty:
        return []
    issues = []
    duplicates = int(df.duplicated(["date", "time"]).sum())
    if duplicates:
        issues.append(f"{duplicates} duplicate slots")
    per_day = pd.to_datetime(df["date"], format="%Y/%m/%d").value_counts().sort_index()
    # Every day has 48 slots (no DST in Japan); the last day of a month in progress has fewer.
    if per_day.iloc[:-1].ne(48).any() or per_day.iloc[-1] > 48:
        issues.append("a complete day without 48 slots")
    if df["solar"].lt(0).any():
        issues.append("negative solar")
    return issues


PARSER = Parser(
    source="tepco_actuals",
    version=VERSION,
    columns=(
        Column("row_number", "INTEGER", required=True),
        Column("date", "STRING", required=True),
        Column("time", "STRING", required=True),
        *(Column(name, "INTEGER") for name in MEASURES),
    ),
    parse=parse,
    check=check,
    reads=lambda filename: filename.startswith("eria_jukyu_"),
)

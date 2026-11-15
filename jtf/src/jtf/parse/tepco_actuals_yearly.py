"""TEPCO's yearly area actuals, `area-YYYY.csv`, FY2016–FY2023: hourly, before the monthly files.

Row 1 is a unit row, `単位[万kWh]` (万kWh in the hour: 10 MWh, so 10 MW on average). The header
spans two rows: `DATE`, `TIME` and the demand column on row 2, then `供給力` ("supply") as a
group label whose columns are named on row 3. FY2016–FY2018 quote numbers with thousands
separators (`"2,555"`), which are formatting, not data: the parser removes them. Thermal power
is one column here, split four ways in the monthly files. FY2023 ends on 31 January 2024, where
the monthly files begin.
"""

from __future__ import annotations

import csv

import pandas as pd

from jtf.parse.base import Column, Parser, RawFile, Refused, whole_numbers

VERSION = "1"
UNIT_ROW = "単位[万kWh]"
COLUMNS = {
    "DATE": "date",
    "TIME": "time",
    "東京エリア需要": "area_demand",
    "原子力": "nuclear",
    "火力": "thermal",
    "水力": "hydro",
    "地熱": "geothermal",
    "バイオマス": "biomass",
    "太陽光発電実績": "solar",
    "太陽光出力制御量": "solar_curtailed",
    "風力発電実績": "wind",
    "風力出力制御量": "wind_curtailed",
    "揚水": "pumped_storage",
    "連系線": "interconnectors",
    "合計": "total",
}
MEASURES = [name for name in COLUMNS.values() if name not in ("date", "time")]


def parse(raw: RawFile) -> pd.DataFrame:
    lines = raw.text().splitlines()
    unit = lines[0].split(",")[0] if lines else ""
    if unit != UNIT_ROW:
        raise Refused(f"unit row is {unit!r}, expected {UNIT_ROW!r}")
    top, below = next(csv.reader([lines[1]])), next(csv.reader([lines[2]]))
    names = top[:3] + below[3:]
    unknown = sorted(set(names) - set(COLUMNS))
    if unknown:
        raise Refused(f"no ASCII name for columns {unknown}")
    rows = [row[: len(names)] for row in csv.reader(lines[3:]) if any(v.strip() for v in row)]
    if any(len(row) < len(names) for row in rows):
        raise Refused("a row has fewer values than the header")
    df = pd.DataFrame(rows, columns=names).rename(columns=COLUMNS)
    df = df.reindex(columns=list(COLUMNS.values()))
    df.insert(0, "row_number", range(1, len(df) + 1))
    for column in MEASURES:
        df[column] = df[column].str.replace(",", "", regex=False)
    return whole_numbers(df, MEASURES).assign(**raw.lineage(VERSION))


def check(df: pd.DataFrame) -> list[str]:
    if df.empty:
        return []
    issues = []
    duplicates = int(df.duplicated(["date", "time"]).sum())
    if duplicates:
        issues.append(f"{duplicates} duplicate hours")
    if pd.to_datetime(df["date"], format="%Y/%m/%d").value_counts().ne(24).any():
        issues.append("a day without 24 hours")
    if df["solar"].lt(0).any():
        issues.append("negative solar")
    return issues


PARSER = Parser(
    source="tepco_actuals",
    table="tepco_actuals_yearly",
    version=VERSION,
    columns=(
        Column("row_number", "INTEGER", required=True),
        Column("date", "STRING", required=True),
        Column("time", "STRING", required=True),
        *(Column(name, "INTEGER") for name in MEASURES),
    ),
    parse=parse,
    check=check,
    reads=lambda filename: filename.startswith("area-"),
)

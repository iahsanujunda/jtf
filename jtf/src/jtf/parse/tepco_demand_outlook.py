"""TEPCO's demand outlook (でんき予報): today's `juyo-d1-j.csv`, and its monthly archive.

One file holds 11 blocks separated by blank lines, of three shapes: nine headline figures for
today and tomorrow (peak supply, predicted maximum demand, the hour of peak usage, the maximum
usage rate), an hourly block (actual demand, TEPCO's forecast, usage rate, supply) and a 5-minute
block (demand, solar output, solar's share). The first line is the file's own update time,
`2026/11/15 12:15 UPDATE`. Values are 万kW (10 MW) or %, whole numbers in every file so far.
Actual demand is 0 until TEPCO confirms it; 5-minute rows are blank until they happen.

The archive, `YYYYMM_power_usage.zip`, holds one end-of-day file a day in the same layout. Its
"tomorrow" figures were updated on the day they describe, so DWD takes each figure's own update
time as its `known_at`. Each table therefore has three parsers over the same files, one per
block shape, and every row names the daily file it came from (`member`, null for today's file).
"""

from __future__ import annotations

import csv
import io
import zipfile

import pandas as pd

from jtf.parse.base import Column, Parser, RawFile, Refused, whole_numbers

VERSION = "1"
LIVE_FILE = "juyo-d1-j.csv"
ARCHIVE_SUFFIX = "_power_usage.zip"
MAX_MEMBER_BYTES = 1_000_000  # a daily file is about 12 KB; anything far bigger isn't one

HOURLY = {
    "DATE": "date",
    "TIME": "time",
    "当日実績(万kW)": "actual_man_kw",
    "予測値(万kW)": "forecast_man_kw",
    "使用率(%)": "usage_pct",
    "供給力(万kW)": "supply_man_kw",
}
FIVE_MINUTE = {
    "DATE": "date",
    "TIME": "time",
    "当日実績(５分間隔値)(万kW)": "actual_man_kw",
    "太陽光発電実績(５分間隔値)(万kW)": "solar_man_kw",
    "太陽光発電量(電力使用量に対する割合)(%)": "solar_share_pct",
}
HEADLINES = {
    "ピーク時供給力(万kW)": "peak_supply",
    "予想最大電力(万kW)": "peak_demand_forecast",
    "使用率ピーク時供給力(万kW)": "usage_peak_supply",
    "使用率ピーク時予想電力(万kW)": "usage_peak_demand_forecast",
    "最大使用率(%)": "max_usage_pct",
}
HEADLINE_FIELDS = ["value", "time_band", "info_date", "info_time", "reserve_pct", "usage_pct"]


def daily_files(raw: RawFile) -> list[tuple[str | None, str]]:
    """(member, text) for each daily file: one for today's file, one a day for an archive zip."""
    if raw.filename == LIVE_FILE:
        return [(None, raw.text())]
    try:
        archive = zipfile.ZipFile(io.BytesIO(raw.body))
    except zipfile.BadZipFile as e:
        raise Refused(f"not a zip: {e}") from e
    files = []
    for info in sorted(archive.infolist(), key=lambda i: i.filename):
        if not info.filename.endswith("_power_usage.csv") or info.file_size > MAX_MEMBER_BYTES:
            raise Refused(f"unexpected member {info.filename!r} ({info.file_size} bytes)")
        files.append((info.filename, archive.read(info).decode("cp932")))
    return files


def _blocks(text: str) -> tuple[pd.Timestamp, list[list[str]]]:
    blocks, current = [], []
    for line in text.splitlines():
        if line.strip():
            current.append(line)
        elif current:
            blocks.append(current)
            current = []
    if current:
        blocks.append(current)
    if not blocks or not blocks[0][0].endswith(" UPDATE"):
        raise Refused("no update line at the top")
    updated = pd.Timestamp(blocks[0][0].removesuffix(" UPDATE"), tz="Asia/Tokyo")
    blocks[0] = blocks[0][1:]
    return updated, blocks


def _header(line: str) -> list[str]:
    names = next(csv.reader([line]))
    while names and not names[-1]:  # some headers end in an empty column
        names.pop()
    return names


def _table(block: list[str], mapping: dict[str, str]) -> pd.DataFrame:
    names = _header(block[0])
    unknown = sorted(set(names) - set(mapping))
    if unknown:
        raise Refused(f"no ASCII name for columns {unknown}")
    rows = [row[: len(names)] + [""] * (len(names) - len(row)) for row in csv.reader(block[1:])]
    df = pd.DataFrame(rows, columns=names).rename(columns=mapping)
    return df.reindex(columns=list(mapping.values()))


def _headlines(blocks: list[list[str]]) -> pd.DataFrame:
    rows = []
    for block in blocks:
        name = _header(block[0])[0]
        figure = HEADLINES.get(name.removeprefix("翌日の"))
        if figure is None:
            raise Refused(f"unknown headline block {name!r}")
        values = next(csv.reader([block[1]])) if len(block) > 1 else []
        fields = dict(zip(HEADLINE_FIELDS, values + [""] * len(HEADLINE_FIELDS), strict=False))
        day = "tomorrow" if name.startswith("翌日の") else "today"
        rows.append({"day": day, "figure": figure, **fields})
    return pd.DataFrame(rows, columns=["day", "figure", *HEADLINE_FIELDS])


def _shape(blocks: list[list[str]], shape: str) -> pd.DataFrame:
    def is_table(block, first_value):
        names = _header(block[0])
        return names[:2] == ["DATE", "TIME"] and len(names) > 2 and names[2] == first_value

    if shape == "hourly":
        found = [b for b in blocks if is_table(b, "当日実績(万kW)")]
        return _table(found[0], HOURLY) if len(found) == 1 else _missing(shape, len(found))
    if shape == "5min":
        found = [b for b in blocks if is_table(b, "当日実績(５分間隔値)(万kW)")]
        return _table(found[0], FIVE_MINUTE) if len(found) == 1 else _missing(shape, len(found))
    return _headlines([b for b in blocks if _header(b[0])[:2] != ["DATE", "TIME"]])


def _missing(shape: str, found: int):
    raise Refused(f"{found} {shape} blocks, expected 1")


def _parser(shape: str, values: list[str], data_columns: tuple[Column, ...]) -> Parser:
    def parse(raw: RawFile) -> pd.DataFrame:
        frames = []
        for member, text in daily_files(raw):
            updated, blocks = _blocks(text)
            df = _shape(blocks, shape)
            df.insert(0, "row_number", range(1, len(df) + 1))
            df.insert(0, "member", member)
            frames.append(df.assign(source_time=updated))
        df = whole_numbers(pd.concat(frames, ignore_index=True), values)
        lineage = raw.lineage(VERSION)
        lineage.pop("source_time")  # each daily file's own update line
        return df.assign(**lineage)

    expected = {"peaks": 9, "hourly": 24, "5min": 288}[shape]
    key = ["member", "day", "figure"] if shape == "peaks" else ["member", "date", "time"]

    def check(df: pd.DataFrame) -> list[str]:
        if df.empty:
            return []
        issues = []
        rows = df.groupby(df["member"].fillna(LIVE_FILE)).size()
        if rows.ne(expected).any():
            issues.append(f"a daily file without {expected} {shape} rows")
        duplicates = int(df.fillna({"member": LIVE_FILE}).duplicated(key).sum())
        if duplicates:
            issues.append(f"{duplicates} duplicate rows")
        return issues

    return Parser(
        source="tepco_demand_outlook",
        table=f"tepco_demand_outlook_{shape}",
        version=VERSION,
        columns=(
            Column("member", "STRING"),
            Column("row_number", "INTEGER", required=True),
            *data_columns,
        ),
        parse=parse,
        check=check,
        reads=lambda name: name == LIVE_FILE or name.endswith(ARCHIVE_SUFFIX),
    )


def _columns(mapping: dict[str, str], whole: list[str]) -> tuple[Column, ...]:
    return tuple(
        Column(name, "INTEGER" if name in whole else "STRING", required=name in ("date", "time"))
        for name in mapping.values()
    )


HOURLY_VALUES = ["actual_man_kw", "forecast_man_kw", "usage_pct", "supply_man_kw"]
FIVE_MINUTE_VALUES = ["actual_man_kw", "solar_man_kw", "solar_share_pct"]
PEAK_VALUES = ["value", "reserve_pct", "usage_pct"]

PARSERS = [
    _parser(
        "peaks",
        PEAK_VALUES,
        (
            Column("day", "STRING", required=True),
            Column("figure", "STRING", required=True),
            Column("value", "INTEGER"),
            Column("time_band", "STRING"),
            Column("info_date", "STRING"),
            Column("info_time", "STRING"),
            Column("reserve_pct", "INTEGER"),
            Column("usage_pct", "INTEGER"),
        ),
    ),
    _parser("hourly", HOURLY_VALUES, _columns(HOURLY, HOURLY_VALUES)),
    _parser("5min", FIVE_MINUTE_VALUES, _columns(FIVE_MINUTE, FIVE_MINUTE_VALUES)),
]

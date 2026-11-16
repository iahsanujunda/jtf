"""METI's FIT/FIP disclosure: installed and certified renewable capacity, every quarter.

Tables A (by prefecture, `A_prefYYYYMM.xlsx`) and B (by municipality, `B_cityYYYYMM.xlsx`) are
workbooks of six sheets: counts and capacity of new and transferred installations, then certified
counts and capacity. Every sheet has its title in row 1, a three-row header in rows 3–5 (category,
size class, "of which" subset, with merged cells left empty) and one row per area. ODS keeps them
semi-long: one row per (sheet, area, header path) with the value as found, kW for capacity and a
count otherwise. An "of which" path is a subset of its parent, never to be added to it. The file
name carries the month the figures describe, unpacked into `ref_date`, the month's last day.

The summary page dates each release in its notices. Every stored copy of it, live or archived in
the Wayback Machine, becomes rows of `meti_fit_notices`, from which DWD takes `known_at`; the
publisher later moved some of its dates earlier (notebooks/03_solar_capacity.ipynb, section 2).
"""

from __future__ import annotations

import hashlib
import io
import re
import unicodedata
from email.utils import parsedate_to_datetime
from html import unescape

import openpyxl
import pandas as pd

from jtf.parse.base import Column, Parser, RawFile, Refused

VERSION = "1"
SOURCE = "meti_fit_capacity"

# Per table: sheet name, title, and a digest of the sheet's header paths. One layout in every
# .xlsx release since June 2017; tables A and B share their headers sheet for sheet.
_COUNTS_NEW, _COUNTS_MOVED = "5921b6727877d023", "dbbb8a22a7fb3021"
_KW_NEW, _KW_MOVED = "c303b13176342b05", "cba20c17428b7802"
LAYOUTS = {
    "A": (
        ("表A①－１", "【表A ①-１】　都道府県別導入件数（新規認定分）", _COUNTS_NEW),
        ("表A①－２", "【表A ①-２】　都道府県別導入件数（移行認定分）", _COUNTS_MOVED),
        ("表A②－１", "【表A ②-１】　都道府県別導入容量（新規認定分）", _KW_NEW),
        ("表A②－２", "【表A ②-２】　都道府県別導入容量（移行認定分）", _KW_MOVED),
        ("表A③", "【表A ③】　都道府県別認定件数（新規認定分）", _COUNTS_NEW),
        ("表A④", "【表A ④】　都道府県別認定容量（新規認定分）", _KW_NEW),
    ),
    "B": (
        ("表B①－１", "【表B ①-１】　市町村別導入件数（新規認定分）", _COUNTS_NEW),
        ("表B①－２", "【表B ①-２】　市町村別導入件数（移行認定分）", _COUNTS_MOVED),
        ("表B②－１", "【表B ②-１】　市町村別導入容量（新規認定分）", _KW_NEW),
        ("表B②－２", "【表B ②-２】　市町村別導入容量（移行認定分）", _KW_MOVED),
        ("表B③", "【表B ③】　市町村別認定件数（新規認定分）", _COUNTS_NEW),
        ("表B④", "【表B ④】　市町村別認定容量（新規認定分）", _KW_NEW),
    ),
}
RELEASE = re.compile(r"^([AB])_(?:pref|city)(\d{4})(\d{2})\.xlsx$")


def header_paths(head: list[tuple]) -> list[str | None]:
    """Join the three header rows; merged cells are empty, so carry labels right in their group."""
    top, mid, low = (list(row) for row in head)
    paths: list[str | None] = [None]
    for j in range(1, len(top)):
        if top[j] is None:
            top[j] = top[j - 1]
            if mid[j] is None:
                mid[j] = mid[j - 1]
        paths.append("/".join(str(x) for x in (top[j], mid[j], low[j]) if x is not None) or None)
    return paths


def _digest(paths: list[str | None]) -> str:
    return hashlib.sha256("\n".join(p for p in paths if p).encode()).hexdigest()[:16]


def parse_release(raw: RawFile) -> pd.DataFrame:
    name = RELEASE.match(raw.filename)
    if not name:
        raise Refused(f"{raw.filename!r} is not a table A or B release")
    table = name[1]
    ref_date = (pd.Timestamp(int(name[2]), int(name[3]), 1) + pd.offsets.MonthEnd()).date()
    try:
        wb = openpyxl.load_workbook(io.BytesIO(raw.body), read_only=True, data_only=True)
    except Exception as e:  # an .xls or a broken download
        raise Refused(f"not an .xlsx workbook: {type(e).__name__}") from e
    if wb.sheetnames != [sheet for sheet, _, _ in LAYOUTS[table]]:
        raise Refused(f"sheets are {wb.sheetnames}")
    out = []
    for ws, (sheet, title, digest) in zip(wb.worksheets, LAYOUTS[table], strict=True):
        rows = list(ws.iter_rows(values_only=True))
        if len(rows) < 6 or rows[0][0] != title:
            raise Refused(f"sheet {sheet} has the title {rows[0][0] if rows else None!r}")
        paths = header_paths(rows[2:5])
        if _digest(paths) != digest:
            raise Refused(f"the header paths of sheet {sheet} changed")
        for number, row in enumerate(rows[5:], start=6):
            if not isinstance(row[0], str):
                continue
            for j, value in enumerate(row[1:], start=1):
                if value is None or j >= len(paths) or not paths[j]:
                    continue
                if isinstance(value, bool) or not isinstance(value, (int, float)):
                    raise Refused(f"sheet {sheet} row {number} has a non-number: {value!r}")
                out.append((ref_date, sheet, number, row[0], paths[j], float(value)))
    df = pd.DataFrame(out, columns=["ref_date", "sheet", "row_number", "area", "path", "value"])
    return df.assign(**raw.lineage(VERSION))


def check_release(rows: pd.DataFrame) -> list[str]:
    problems = []
    if len(rows) and not (rows["area"] == "合計").any():
        problems.append("no national total (合計) row")
    if (rows["value"] < 0).any():
        problems.append(f"{int((rows['value'] < 0).sum())} negative values")
    return problems


_NOTICE = re.compile(r"^(\d{4})\.(\d{1,2})\.(\d{1,2}) ?>? ?(\d{4})年(\d{1,2})月末時点.*公表")
_HEADING = re.compile(r"■(\d{4})年(\d{1,2})月末時点の状況\((\d{4})年(\d{1,2})月(\d{1,2})日更新\)")


def _month_end(year: str, month: str):
    return (pd.Timestamp(int(year), int(month), 1) + pd.offsets.MonthEnd()).date()


def _lines(page: bytes) -> list[str]:
    """The page's visible text, one line per element, full-width characters folded (NFKC)."""
    html = re.sub(r"<(script|style)[^>]*>.*?</\1>", "", page.decode("utf-8", "replace"), flags=re.S)
    lines = unescape(re.sub(r"<[^>]+>", "\n", html)).splitlines()
    return [unicodedata.normalize("NFKC", line).strip() for line in lines if line.strip()]


def parse_notices(raw: RawFile) -> pd.DataFrame:
    """Each announcement the page shows: a notice line, or the heading above the national totals.

    A notice's date and text can sit on one line or two, so each line is also tried joined to the
    next. An archived copy's `source_time` is when the archive took it (`Memento-Datetime`).
    """
    lines = _lines(raw.body)
    seen, out = set(), []
    for number, (line, after) in enumerate(zip(lines, [*lines[1:], ""], strict=True), start=1):
        for text in (line, f"{line} {after}"):
            if (m := _NOTICE.match(text)) and ("notice", m[0]) not in seen:
                seen.add(("notice", m[0]))
                announced = pd.Timestamp(int(m[1]), int(m[2]), int(m[3])).date()
                out.append(("notice", _month_end(m[4], m[5]), announced, number, m[0]))
        if (m := _HEADING.search(line)) and ("heading", m[0]) not in seen:
            seen.add(("heading", m[0]))
            announced = pd.Timestamp(int(m[3]), int(m[4]), int(m[5])).date()
            out.append(("heading", _month_end(m[1], m[2]), announced, number, m[0]))
    headers = {k.lower(): v for k, v in raw.meta.get("response_headers", {}).items()}
    memento = headers.get("memento-datetime")
    taken = pd.Timestamp(parsedate_to_datetime(memento)) if memento else None
    df = pd.DataFrame(out, columns=["kind", "ref_date", "announced", "row_number", "text"])
    return df.assign(**raw.lineage(VERSION, source_time=taken))


def check_notices(rows: pd.DataFrame) -> list[str]:
    if len(rows) and not (rows["kind"] == "notice").any():
        return ["a heading but no notice lines"]
    return []


RELEASE_COLUMNS = (
    Column("ref_date", "DATE", required=True),
    Column("sheet", "STRING", required=True),
    Column("row_number", "INTEGER", required=True),
    Column("area", "STRING", required=True),
    Column("path", "STRING", required=True),
    Column("value", "FLOAT", required=True),
)
PARSERS = (
    Parser(
        SOURCE,
        VERSION,
        RELEASE_COLUMNS,
        parse_release,
        check_release,
        reads=lambda filename: filename.startswith("A_pref") and filename.endswith(".xlsx"),
        table="meti_fit_capacity_pref",
    ),
    Parser(
        SOURCE,
        VERSION,
        RELEASE_COLUMNS,
        parse_release,
        check_release,
        reads=lambda filename: filename.startswith("B_city") and filename.endswith(".xlsx"),
        table="meti_fit_capacity_city",
    ),
    Parser(
        SOURCE,
        VERSION,
        (
            Column("kind", "STRING", required=True),
            Column("ref_date", "DATE", required=True),
            Column("announced", "DATE", required=True),
            Column("row_number", "INTEGER", required=True),
            Column("text", "STRING", required=True),
        ),
        parse_notices,
        check_notices,
        reads=lambda filename: filename.endswith("PublicInfoSummary.html"),
        table="meti_fit_notices",
    ),
)

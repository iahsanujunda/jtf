"""What every parser shares: the raw file it reads, how it refuses one, and the lineage columns."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from email.utils import parsedate_to_datetime

import pandas as pd

from jtf.ingest.landed import SIDECAR
from jtf.storage import Store


class Refused(Exception):
    """The file's shape isn't what the parser knows. Nothing from it is loaded."""


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    """BigQuery type: STRING, INTEGER, FLOAT, BOOLEAN, DATE or TIMESTAMP."""
    required: bool = False


# On every ODS row: where it came from and when we had it. DWD derives versions and
# `known_at` from these; ODS itself never interprets them.
LINEAGE = (
    Column("source_file", "STRING", required=True),
    Column("extraction_date", "DATE", required=True),
    Column("fetched_at", "TIMESTAMP", required=True),
    Column("load_type", "STRING", required=True),
    Column("source_time", "TIMESTAMP"),
    Column("parser_version", "STRING", required=True),
)
# Added by the load, not the parser.
LOAD = (
    Column("ingested_at", "TIMESTAMP", required=True),
    Column("batch_id", "STRING", required=True),
)


@dataclass(frozen=True)
class RawFile:
    path: str
    meta: dict
    body: bytes

    @property
    def filename(self) -> str:
        return self.meta["filename"]

    def text(self) -> str:
        """TEPCO and JEPX files are UTF-8 or CP932 (Shift_JIS), varying from file to file."""
        try:
            return self.body.decode("utf-8-sig")
        except UnicodeDecodeError:
            return self.body.decode("cp932")

    def last_modified(self) -> pd.Timestamp | None:
        headers = {k.lower(): v for k, v in self.meta.get("response_headers", {}).items()}
        value = headers.get("last-modified")
        return pd.Timestamp(parsedate_to_datetime(value)) if value else None

    def lineage(self, parser_version: str, source_time: pd.Timestamp | None = None) -> dict:
        """`source_time` is the update time written in the file, else `Last-Modified`."""
        if source_time is None:
            source_time = self.last_modified()
        return {
            "source_file": self.path,
            "extraction_date": date.fromisoformat(self.meta["extraction_date"]),
            "fetched_at": pd.Timestamp(self.meta["fetched_at"]),
            "load_type": self.meta.get("load_type", "daily"),
            "source_time": source_time if source_time is not None else pd.NaT,
            "parser_version": parser_version,
        }


WHOLE_NUMBER = r"-?\d+"


def whole_numbers(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Type columns as whole numbers. Every file seen so far has only those there, so a decimal
    or text is refused: it most likely means the unit or the layout changed."""
    for column in columns:
        values = df[column].replace("", None)
        present = values.dropna().astype(str)
        odd = present[~present.str.fullmatch(WHOLE_NUMBER)]
        if len(odd):
            raise Refused(f"{column} has a value that is not a whole number: {odd.iloc[0]!r}")
        df[column] = pd.to_numeric(values).astype("Int64")
    return df


def decimals(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for column in columns:
        try:
            df[column] = pd.to_numeric(df[column].replace("", None)).astype("Float64")
        except (ValueError, TypeError) as e:
            raise Refused(f"{column} has a value that is not a number: {e}") from e
    return df


def read_raw(store: Store, path: str) -> RawFile:
    return RawFile(path, json.loads(store.get(path + SIDECAR)), store.get(path))


@dataclass(frozen=True)
class Parser:
    """One file layout: how to read it into its ODS table, and what to check per file."""

    source: str
    version: str
    """Changing what the parser writes bumps this; a re-parse adds rows under the new version."""
    columns: tuple[Column, ...]
    """The data columns; LINEAGE and LOAD follow them in the table."""
    parse: Callable[[RawFile], pd.DataFrame]
    check: Callable[[pd.DataFrame], list[str]]
    """Problems in a parsed file that don't stop the load but should be looked at."""
    reads: Callable[[str], bool] = lambda _filename: True
    """Which files under the source's prefix are in this parser's layout; others have their own."""
    table: str = ""
    """The ODS table; the source's name unless the source has several layouts."""

    def __post_init__(self):
        if not self.table:
            object.__setattr__(self, "table", self.source)

    @property
    def schema(self) -> tuple[Column, ...]:
        return (*self.columns, *LINEAGE, *LOAD)


def conform(rows: pd.DataFrame, schema: tuple[Column, ...]) -> pd.DataFrame:
    """Exactly the table's columns, in order, with types BigQuery loads without guessing."""
    names = [c.name for c in schema]
    if set(rows.columns) != set(names):
        raise ValueError(
            f"columns differ from the table: missing {sorted(set(names) - set(rows.columns))}, "
            f"extra {sorted(set(rows.columns) - set(names))}"
        )
    out = rows[names].copy()
    for column in schema:
        values = out[column.name]
        if column.type == "TIMESTAMP":
            out[column.name] = pd.to_datetime(values, utc=True)
        elif column.type == "DATE":
            out[column.name] = pd.to_datetime(values).dt.date
        elif column.type == "INTEGER":
            out[column.name] = values.astype("Int64")
        elif column.type == "FLOAT":
            out[column.name] = values.astype("Float64")
        elif column.type == "BOOLEAN":
            out[column.name] = values.astype("boolean")
        else:
            out[column.name] = values.astype("string")
    return out

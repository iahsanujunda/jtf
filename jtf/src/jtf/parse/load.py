"""Parse one extraction day of one source into its ODS table, once per parser version.

Every file gets a row in the manifest, `raw_files`, whatever happened to it. The manifest is also
the record of what is done: a re-run skips files it lists for this parser version, so loading a
day twice adds nothing. A file already in the ODS table but missing from the manifest (a load
that stopped between the writes) gets its manifest row without loading its rows again.

Three writes, in order: the rows, each loaded file's fingerprint (`fingerprint.py`), the manifest.
A load stopped after the rows is finished by the next parse: the fingerprint of a file found in
the table but not in the manifest is recorded then, unless it already was.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Protocol

import pandas as pd

from jtf.ingest.landed import SIDECAR
from jtf.parse.base import LOAD, Column, Parser, RawFile, Refused, conform, read_raw
from jtf.parse.fingerprint import Fingerprinted, record
from jtf.storage import Store

MANIFEST = "raw_files"
MANIFEST_COLUMNS = (
    Column("path", "STRING", required=True),
    Column("source", "STRING", required=True),
    Column("ods_table", "STRING", required=True),
    Column("extraction_date", "DATE", required=True),
    Column("filename", "STRING", required=True),
    Column("fetched_at", "TIMESTAMP", required=True),
    Column("load_type", "STRING", required=True),
    Column("http_status", "INTEGER"),
    Column("gap", "BOOLEAN", required=True),
    Column("size_bytes", "INTEGER", required=True),
    Column("sha256", "STRING", required=True),
    Column("last_modified", "STRING"),
    Column("status", "STRING", required=True),
    Column("row_count", "INTEGER", required=True),
    Column("check_result", "STRING"),
    Column("note", "STRING"),
    Column("parser_version", "STRING", required=True),
    *LOAD,
)


class Warehouse(Fingerprinted, Protocol):
    def manifest_files(self, table: str, version: str, day: date) -> set[str]:
        """Files the manifest lists for this table, parser version and day, failures excluded."""
        ...

    def table_files(self, table: str, version: str, day: date) -> set[str]:
        """Files that have rows in the ODS table for this parser version and day."""
        ...

    def append(
        self, table: str, schema: tuple[Column, ...], cluster_by: str, rows: pd.DataFrame
    ) -> None: ...


@dataclass
class DayResult:
    source: str
    day: str
    loaded: list[str] = field(default_factory=list)
    rows: int = 0
    skipped: int = 0
    empty: list[str] = field(default_factory=list)
    gaps: list[str] = field(default_factory=list)
    flagged: dict[str, str] = field(default_factory=dict)
    failed: dict[str, str] = field(default_factory=dict)
    recorded: int = 0  # rows written to the manifest: every file not skipped
    fingerprinted: int = 0  # files whose fingerprint was recorded

    @property
    def ok(self) -> bool:
        return not self.failed

    def summary(self) -> str:
        lines = [
            f"source={self.source} day={self.day} loaded={len(self.loaded)} rows={self.rows} "
            f"skipped={self.skipped} empty={len(self.empty)} gaps={len(self.gaps)} "
            f"flagged={len(self.flagged)} failed={len(self.failed)}"
        ]
        lines += [f"FAILED {path}: {reason}" for path, reason in self.failed.items()]
        lines += [f"flagged {path}: {issues}" for path, issues in self.flagged.items()]
        return "\n".join(lines)


def landed_paths(store: Store, parser: Parser, day: str) -> list[str]:
    """The day's files of the parser's layout that have their sidecar, oldest first."""
    listed = store.list(f"{parser.source}/{day}/")
    present = set(listed)
    return [
        p
        for p in listed
        if not p.endswith(SIDECAR) and p + SIDECAR in present and parser.reads(_filename(p))
    ]


def _filename(path: str) -> str:
    """`src/day/20261115T020416Z_name.csv` -> `name.csv`."""
    return path.rsplit("/", 1)[-1].partition("_")[2]


def parse_day(
    parser: Parser,
    store: Store,
    warehouse: Warehouse,
    day: str,
    *,
    batch_id: str,
    now: datetime,
) -> DayResult:
    when = date.fromisoformat(day)
    done = warehouse.manifest_files(parser.table, parser.version, when)
    in_table = warehouse.table_files(parser.table, parser.version, when)
    result = DayResult(parser.source, day)
    frames, entries, unfinished = [], [], []
    for path in landed_paths(store, parser, day):
        if path in done:
            result.skipped += 1
            continue
        raw = read_raw(store, path)
        entry = {"status": "parsed", "row_count": 0, "check_result": None, "note": None}
        if raw.meta.get("gap"):
            entry.update(status="gap", note="recorded gap: the source had no such item")
            result.gaps.append(path)
        else:
            try:
                rows = parser.parse(raw)
            except Refused as e:
                entry.update(status="failed", note=str(e))
                result.failed[path] = str(e)
            else:
                issues = parser.check(rows)
                entry.update(
                    status="parsed" if len(rows) else "empty",
                    row_count=len(rows),
                    check_result="; ".join(issues) or "ok",
                )
                if issues:
                    result.flagged[path] = "; ".join(issues)
                if not len(rows):
                    result.empty.append(path)
                elif path not in in_table:
                    frames.append(rows)
                    result.loaded.append(path)
                    result.rows += len(rows)
                else:  # its rows are in the table, but its load stopped before the manifest
                    unfinished.append(path)
        entries.append(_manifest_row(parser, raw, entry))

    stamp = {"ingested_at": now, "batch_id": batch_id}
    if frames:
        rows = pd.concat(frames, ignore_index=True).assign(**stamp)
        warehouse.append(parser.table, parser.schema, "source_file", conform(rows, parser.schema))
    if result.loaded or unfinished:
        result.fingerprinted = record(
            warehouse,
            parser.table,
            when,
            set(result.loaded) | set(unfinished),
            version=parser.version,
            batch_id=batch_id,
        )
    if entries:
        manifest = pd.DataFrame(entries).assign(**stamp)
        warehouse.append(MANIFEST, MANIFEST_COLUMNS, "source", conform(manifest, MANIFEST_COLUMNS))
        result.recorded = len(entries)
    return result


def _manifest_row(parser: Parser, raw: RawFile, entry: dict) -> dict:
    meta = raw.meta
    headers = {k.lower(): v for k, v in meta.get("response_headers", {}).items()}
    return {
        "path": raw.path,
        "source": parser.source,
        "ods_table": parser.table,
        "extraction_date": meta["extraction_date"],
        "filename": raw.filename,
        "fetched_at": meta["fetched_at"],
        "load_type": meta.get("load_type", "daily"),
        "http_status": meta.get("http_status"),
        "gap": bool(meta.get("gap")),
        "size_bytes": meta["size_bytes"],
        "sha256": meta["sha256"],
        "last_modified": headers.get("last-modified"),
        "parser_version": parser.version,
        **entry,
    }

"""Each file's rows in ODS, fingerprinted at their load, and the check that they still match.

ODS is append-only, but nothing in BigQuery can enforce it: a row changed in place can only be
noticed (notebooks/07_ods_fingerprints.ipynb). After a parse appends a file's rows, BigQuery
fingerprints them and the loader records the fingerprint in `fingerprints`, beside the manifest.
The check after every load reads only the partitions whose free metadata moved since their last
record: a row count that differs from the recorded ones, or a change after the last record. Both,
because BigQuery warns that deletions might not move the time. Files already in ODS when this
began are fingerprinted once by backfill, and their record says so: they are guarded from then on.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Protocol

import numpy as np
import pandas as pd

from jtf.parse.base import Column, conform

FINGERPRINTS = "fingerprints"
FINGERPRINT_COLUMNS = (
    Column("ods_table", "STRING", required=True),
    Column("source_file", "STRING", required=True),
    Column("parser_version", "STRING", required=True),
    Column("extraction_date", "DATE", required=True),
    Column("row_count", "INTEGER", required=True),
    Column("fingerprint", "STRING", required=True),
    Column("basis", "STRING", required=True),  # load or backfill
    Column("recorded_at", "TIMESTAMP", required=True),  # BigQuery's time when computed
    Column("batch_id", "STRING", required=True),
)
# Each row as JSON without its nulls, so a column added later leaves old rows alone; hashed, and
# summed exactly, so the order rows are read in doesn't matter and a file loaded twice shows.
ROW_JSON = "TO_JSON_STRING(JSON_STRIP_NULLS(TO_JSON(t)))"
FINGERPRINT_SQL = f"CAST(SUM(CAST(FARM_FINGERPRINT({ROW_JSON}) AS BIGNUMERIC)) AS STRING)"
KEY = ["source_file", "parser_version"]


class Fingerprinted(Protocol):
    def fingerprints(
        self, table: str, days: list[date] | None, files: list[str] | None = None
    ) -> pd.DataFrame:
        """Per file and version in the partitions `days` (every partition if None), optionally
        only `files`: extraction_date, row_count, fingerprint, and recorded_at, the warehouse's
        own time when they were computed."""
        ...

    def partitions(self, table: str) -> pd.DataFrame:
        """Per partition: extraction_date, total_rows, last_modified."""
        ...

    def recorded(self, table: str) -> pd.DataFrame:
        """The rows of `fingerprints` for one table."""
        ...

    def append(
        self, table: str, schema: tuple[Column, ...], cluster_by: str, rows: pd.DataFrame
    ) -> None: ...


def record(
    warehouse: Fingerprinted, table: str, day: date, files: set[str], *, version: str, batch_id: str
) -> int:
    """Record the fingerprints of files just loaded (or found loaded without a manifest row) on
    one day, those already recorded for this version left alone. Returns how many."""
    held = warehouse.recorded(table)
    if not held.empty:
        mine = (held.parser_version == version) & (held.extraction_date == day)
        files = files - set(held.loc[mine, "source_file"])
    if not files:
        return 0
    current = warehouse.fingerprints(table, [day], sorted(files))
    current = current[current.parser_version == version]
    _append(warehouse, table, current, "load", batch_id)
    return len(current)


def backfill(warehouse: Fingerprinted, table: str, *, batch_id: str) -> int:
    """Fingerprint every file of `table` with no record yet, as `backfill`. Run with Dagster
    quiet, as `jtf parse`: a file a parse loads meanwhile could otherwise be recorded twice."""
    current = warehouse.fingerprints(table, None)
    held = warehouse.recorded(table)  # read after, so a record made meanwhile counts
    if not held.empty:
        seen = current.merge(held[KEY].drop_duplicates(), on=KEY, how="left", indicator=True)
        current = current[(seen["_merge"] == "left_only").to_numpy()]
    _append(warehouse, table, current, "backfill", batch_id)
    return len(current)


def _append(warehouse: Fingerprinted, table: str, current: pd.DataFrame, basis: str, batch: str):
    if current.empty:
        return
    rows = current.assign(ods_table=table, basis=basis, batch_id=batch)
    warehouse.append(
        FINGERPRINTS, FINGERPRINT_COLUMNS, "ods_table", conform(rows, FINGERPRINT_COLUMNS)
    )


def screen(partitions: pd.DataFrame, recorded: pd.DataFrame) -> list[date]:
    """The partitions that may have changed since their last record: rows other than the
    recorded counts, a change after the last record, or no record at all; and recorded days
    whose partition is gone."""
    expected = recorded.groupby("extraction_date").agg(
        rows=("row_count", "sum"), last_recorded=("recorded_at", "max")
    )
    both = partitions.set_index("extraction_date").join(expected, how="outer")
    moved = ~(both["last_modified"] <= both["last_recorded"]).fillna(False)
    flagged = both.index[(both["total_rows"] != both["rows"]).to_numpy() | moved.to_numpy()]
    return sorted(flagged)


@dataclass
class Verification:
    table: str
    partitions: int  # partitions screened
    reread: list[date]  # partitions whose rows were fingerprinted again
    problems: dict[str, str]  # "<file> (<version>)" => what is wrong

    @property
    def ok(self) -> bool:
        return not self.problems

    def summary(self) -> str:
        state = "as loaded" if self.ok else f"{len(self.problems)} file(s) not as loaded"
        return f"{self.table}: {state}; {self.partitions} partitions, {len(self.reread)} reread"


def verify(warehouse: Fingerprinted, table: str, *, everything: bool = False) -> Verification:
    """Compare files with their recorded fingerprints: only in the partitions the screen flags,
    or in every partition with `everything` (a full recheck: it reads the whole table)."""
    recorded = warehouse.recorded(table)
    if recorded.empty:
        recorded = pd.DataFrame(columns=[c.name for c in FINGERPRINT_COLUMNS])
    partitions = warehouse.partitions(table)
    days = sorted(partitions["extraction_date"]) if everything else screen(partitions, recorded)

    counts = recorded.groupby(KEY).size()
    twice = counts[counts > 1]
    problems = {
        f"{path} ({version})": f"recorded {n} times" for (path, version), n in twice.items()
    }
    if days:
        now = warehouse.fingerprints(table, days)
        then = recorded[recorded["extraction_date"].isin(days)].drop_duplicates(KEY, keep="last")
        both = now.merge(then, on=KEY, how="outer", suffixes=("_now", "_then"), indicator=True)
        changed = (both["_merge"] == "both") & (both["fingerprint_now"] != both["fingerprint_then"])
        rows = (
            both["row_count_then"].astype("string")
            + " rows then, "
            + both["row_count_now"].astype("string")
            + " now"
        )
        both["problem"] = np.select(
            [both["_merge"] == "left_only", both["_merge"] == "right_only", changed],
            [
                "no fingerprint recorded",
                "its rows are gone",
                "rows changed since recorded: " + rows,
            ],
            default="",
        )
        found = both[both["problem"] != ""]
        names = found["source_file"] + " (" + found["parser_version"].astype("string") + ")"
        # A file recorded twice keeps that message: it explains the rest
        problems = dict(zip(names, found["problem"], strict=True)) | problems
    return Verification(table, len(partitions), days, problems)

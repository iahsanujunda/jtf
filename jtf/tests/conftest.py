import os
from collections import defaultdict
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

FIXTURES = Path(__file__).parent / "fixtures" / "raw"

# Loading the Dagster definitions parses the dbt project when its manifest is missing (as in CI).
# Parsing reads no data, but the profile names these settings, so they need some value.
for name, value in {
    "JTF_BIGQUERY_PROJECT": "test-project",
    "JTF_BIGQUERY_LOCATION": "asia-northeast1",
    "JTF_DBT_DATASET_PREFIX": "test",
}.items():
    os.environ.setdefault(name, value)


class FakeWarehouse:
    """The ODS dataset in memory: each table is the list of frames appended to it. A clock that
    ticks at every append and every fingerprint stands in for BigQuery's, so a partition's last
    change and a fingerprint's `recorded_at` compare as they do there."""

    def __init__(self):
        self.tables: dict[str, list[pd.DataFrame]] = defaultdict(list)
        self.modified: dict[tuple[str, date], pd.Timestamp] = {}
        self.ticks = 0
        self.fingerprinted_days: list[list[date] | None] = []  # what each fingerprint read

    def tick(self) -> pd.Timestamp:
        self.ticks += 1
        return pd.Timestamp("2026-11-15 09:00", tz="UTC") + pd.Timedelta(seconds=self.ticks)

    def change(self, table: str, edit, *, moves: bool = True) -> None:
        """Rewrite a table's rows in place, as an UPDATE or DELETE would; `moves=False` for a
        change BigQuery's partition time might not show (deletions, its docs warn)."""
        before = self.rows(table)
        after = edit(before.copy())
        self.tables[table] = [after]
        if moves:  # as BigQuery does: only the partitions whose rows changed

            def by_day(rows):
                hashes = pd.util.hash_pandas_object(rows, index=False).astype(object)
                return hashes.groupby(rows["extraction_date"]).sum()

            then, now = by_day(before), by_day(after)
            touched = then.index[~then.eq(now.reindex(then.index))]
            stamp = self.tick()
            self.modified.update({(table, day): stamp for day in touched})

    def rows(self, table: str) -> pd.DataFrame:
        frames = self.tables.get(table)
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def manifest_files(self, table, version, day):
        m = self.rows("raw_files")
        if m.empty:
            return set()
        keep = (
            (m["extraction_date"] == day)
            & (m["ods_table"] == table)
            & (m["parser_version"] == version)
            & (m["status"] != "failed")
        )
        return set(m.loc[keep, "path"])

    def table_files(self, table, version, day):
        t = self.rows(table)
        if t.empty:
            return set()
        keep = (t["extraction_date"] == day) & (t["parser_version"] == version)
        return set(t.loc[keep, "source_file"])

    def append(self, table, schema, cluster_by, rows):
        assert list(rows.columns) == [c.name for c in schema]
        self.tables[table].append(rows)
        now = self.tick()
        for day in set(rows["extraction_date"]):
            self.modified[(table, day)] = now

    def fingerprints(self, table, days, files=None):
        self.fingerprinted_days.append(days)
        t = self.rows(table)
        columns = ["source_file", "parser_version", "extraction_date", "row_count", "fingerprint"]
        if t.empty:
            return pd.DataFrame(columns=[*columns, "recorded_at"])
        if days is not None:
            t = t[t["extraction_date"].isin(days)]
        if files is not None:
            t = t[t["source_file"].isin(files)]
        # Per-row hashes summed as Python ints: in any order the same, and a duplicate counts
        hashes = pd.util.hash_pandas_object(t, index=False).astype(object)
        keys = [t["source_file"], t["parser_version"], t["extraction_date"]]
        out = hashes.groupby(keys).agg(row_count="size", fingerprint="sum").reset_index()
        out.columns = columns
        return out.assign(fingerprint=out["fingerprint"].astype(str), recorded_at=self.tick())

    def partitions(self, table):
        t = self.rows(table)
        if t.empty:
            return pd.DataFrame(columns=["extraction_date", "total_rows", "last_modified"])
        out = t.groupby("extraction_date").size().rename("total_rows").reset_index()
        return out.assign(last_modified=[self.modified[(table, d)] for d in out["extraction_date"]])

    def recorded(self, table):
        f = self.rows("fingerprints")
        return f[f["ods_table"] == table].reset_index(drop=True) if not f.empty else f


@pytest.fixture
def warehouse():
    return FakeWarehouse()

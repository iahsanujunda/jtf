from collections import defaultdict
from pathlib import Path

import pandas as pd
import pytest

FIXTURES = Path(__file__).parent / "fixtures" / "raw"


class FakeWarehouse:
    """The ODS dataset in memory: each table is the list of frames appended to it."""

    def __init__(self):
        self.tables: dict[str, list[pd.DataFrame]] = defaultdict(list)

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


@pytest.fixture
def warehouse():
    return FakeWarehouse()

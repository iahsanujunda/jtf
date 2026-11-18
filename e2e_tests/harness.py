"""The e2e tests' world in BigQuery (README.md, The test world), and what the tests do with it:
parse raw files into its ODS, run dbt on it, plant files, and fingerprint what it holds."""

import hashlib
import json
import shutil
import subprocess
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pytest
from google.cloud import bigquery

from jtf.ingest.landed import SIDECAR
from jtf.ml.warehouse import BacktestWarehouse
from jtf.parse import PARSERS
from jtf.parse.bigquery import BigQueryWarehouse
from jtf.parse.fingerprint import FINGERPRINT_SQL
from jtf.parse.load import parse_day
from jtf.storage import LocalStore

HERE = Path(__file__).parent
JST = ZoneInfo("Asia/Tokyo")
WORLD, LATE = HERE / "world" / "raw", HERE / "world" / "late"
DBT_PROJECT = HERE.parent / "warehouse" / "dbt"
FIRST_DAY, LAST_DAY = date(2026, 11, 15), date(2026, 11, 19)
# The decision times of the window, 09:00 JST on the 14th to the 18th
DECISION_TIMES = [pd.Timestamp(f"2026-11-{d} 09:00", tz=JST) for d in range(14, 19)]
# The window's dbt variables: its first target day, and the sun computed for November only. So the
# test against the reference positions, which has days all year, is left out
WINDOW = {
    "first_target_day": FIRST_DAY.isoformat(),
    "sun_first_day": "2026-11-01",
    "sun_last_day": "2026-11-30",
}
NOT_IN_THE_WINDOW = ["dim_sun_slots__matches_spa"]
# Runs record the code they came from; the e2e tests run the code under test, whatever its tree
CODE = {"git_commit": "e2e", "git_dirty": "false", "image_digest": ""}


class World:
    def __init__(self, project: str, location: str, prefix: str, target_path: Path):
        self.project, self.location, self.prefix = project, location, prefix
        self.client = bigquery.Client(project=project, location=location)
        self.target_path = target_path

    def table(self, layer: str, name: str) -> str:
        return f"{self.project}.{self.prefix}_{layer}.{name}"

    def newest(self, source: str, name: str) -> Path:
        """The world's newest file of `source` whose name ends with `name`."""
        files = (WORLD / source).rglob(f"*{name}")
        return sorted(f for f in files if not f.name.endswith(SIDECAR))[-1]

    def ods(self) -> BigQueryWarehouse:
        return BigQueryWarehouse(self.project, f"{self.prefix}_ods", client=self.client)

    def warehouse(self) -> BacktestWarehouse:
        return BacktestWarehouse(self.project, self.location, self.prefix, client=self.client)

    def query(self, sql: str, params: list | None = None) -> pd.DataFrame:
        config = bigquery.QueryJobConfig(query_parameters=params or [])
        return self.client.query(sql, job_config=config).to_dataframe()

    def parse(self, root: Path, *, batch_id: str) -> int:
        """Every raw file under `root` into the ODS through its parser, the parsers side by side:
        a parse is BigQuery's round trips, a few per file. Returns the rows loaded."""
        store, now = LocalStore(root.resolve()), datetime.now(UTC)
        landed = {
            (f.parent.parent.name, f.parent.name)
            for f in root.rglob("*")
            if f.is_file() and not f.name.endswith(SIDECAR)
        }

        def parse(parser):
            client = bigquery.Client(project=self.project, location=self.location)
            ods = BigQueryWarehouse(self.project, f"{self.prefix}_ods", client=client)
            days = sorted(day for source, day in landed if source == parser.source)
            return [parse_day(parser, store, ods, d, batch_id=batch_id, now=now) for d in days]

        with ThreadPoolExecutor(max_workers=len(PARSERS)) as pool:
            results = [r for rs in pool.map(parse, PARSERS.values()) for r in rs]
        failed = {path: why for r in results for path, why in r.failed.items()}
        assert not failed, f"the parsers refused files: {failed}"
        return sum(r.rows for r in results)

    def dbt(self, *args: str) -> str:
        """dbt on the world's datasets, as `dbt <args>`; its output, or a failure with it."""
        variables = json.dumps({"ods_dataset": f"{self.prefix}_ods", **WINDOW})
        command = [shutil.which("dbt") or "dbt", *args, "--target", "e2e", "--profiles-dir", "."]
        command += ["--target-path", str(self.target_path), "--vars", variables]
        done = subprocess.run(command, cwd=DBT_PROJECT, capture_output=True, text=True)
        if done.returncode:
            pytest.fail(f"dbt {' '.join(args)} failed:\n{done.stdout[-6000:]}{done.stderr}")
        return done.stdout

    def plant(
        self,
        root: Path,
        world_file: Path,
        *,
        fetched_at: datetime,
        load_type: str,
        change: Callable[[bytes], bytes] = lambda body: body,
    ) -> str:
        """A copy of a world file, changed by `change`, landed under `root` as if fetched at
        `fetched_at`: the raw bucket's layout, with its sidecar. Returns its path in the store."""
        meta = json.loads(world_file.with_name(world_file.name + SIDECAR).read_text())
        body = change(world_file.read_bytes())
        name = world_file.name.partition("_")[2]
        day = fetched_at.astimezone(JST).date().isoformat()
        path = f"{meta['source']}/{day}/{fetched_at:%Y%m%dT%H%M%SZ}_{name}"
        meta |= {
            "fetched_at": fetched_at.isoformat(),
            "extraction_date": day,
            "load_type": load_type,
            "size_bytes": len(body),
            "sha256": hashlib.sha256(body).hexdigest(),
            "planted": f"by the e2e tests, from {world_file.name}",
        }
        (root / path).parent.mkdir(parents=True, exist_ok=True)
        (root / path).write_bytes(body)
        (root / (path + SIDECAR)).write_text(json.dumps(meta, ensure_ascii=False, indent=1))
        return path

    def tables(self, *layers: str) -> list[str]:
        return [
            f"{self.prefix}_{layer}.{t.table_id}"
            for layer in layers
            for t in self.client.list_tables(f"{self.project}.{self.prefix}_{layer}")
            if "__dbt_tmp" not in t.table_id
        ]

    def fingerprints(self) -> pd.DataFrame:
        """Every table dbt builds, with its rows and fingerprint."""
        parts = [
            f"select '{name}' as model, count(*) as row_count, {FINGERPRINT_SQL} as fingerprint "
            f"from `{self.project}.{name}` as t"
            for name in self.tables("dim", "dwd", "dws", "ads")
        ]
        return self.query(" union all ".join(parts)).sort_values("model", ignore_index=True)

    def seen(self) -> pd.DataFrame:
        """What each decision time sees: every DWS model's rows per decision time, fingerprinted."""
        parts = [
            f"select '{name}' as model, decision_time, count(*) as row_count, "
            f"{FINGERPRINT_SQL} as fingerprint from `{self.project}.{name}` as t "
            "group by decision_time"
            for name in self.tables("dws")
        ]
        rows = self.query(" union all ".join(parts))
        return rows.sort_values(["model", "decision_time"], ignore_index=True)


def differences(before: pd.DataFrame, after: pd.DataFrame, key: list[str]) -> pd.DataFrame:
    """The rows of `before` and `after` that are not in both, `_merge` saying which side has it."""
    both = before.merge(after, how="outer", indicator=True)
    return both[both["_merge"] != "both"].sort_values(key)


def assert_unchanged(before: pd.DataFrame, after: pd.DataFrame, key: list[str]) -> None:
    """Fail unless `before` and `after` hold the same rows, naming the keys that changed."""
    changed = differences(before, after, key)
    sides = {"left_only": "before", "right_only": "after"}
    listed = changed[key].assign(only=changed["_merge"].map(sides)).to_string(index=False)
    assert changed.empty, f"{changed[key].drop_duplicates().shape[0]} changed:\n{listed}"

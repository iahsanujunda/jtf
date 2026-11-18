"""dbt's manifest for the rules: parsed afresh (offline), or with every model's SQL compiled."""

import json
import os
from pathlib import Path

import pytest
from dbt.cli.main import dbtRunner

DBT_DIR = Path(__file__).resolve().parents[1]
SETTINGS = ("JTF_BIGQUERY_PROJECT", "JTF_BIGQUERY_LOCATION", "JTF_DBT_DATASET_PREFIX")


def dbt(command: str, target: Path) -> dict:
    args = [command, "--quiet", "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)]
    result = dbtRunner().invoke([*args, "--target-path", str(target)])
    assert result.success, result.exception
    return json.loads((target / "manifest.json").read_text())


@pytest.fixture(scope="session")
def manifest(tmp_path_factory) -> dict:
    """The project as it is on disk, parsed afresh into a temporary directory: a rule checked
    against a stale manifest could pass on SQL that no longer exists. Parsing reads no data, but
    the profile names the settings, so they need some value."""
    for name, value in zip(SETTINGS, ("test-project", "asia-northeast1", "test"), strict=True):
        os.environ.setdefault(name, value)
    return dbt("parse", tmp_path_factory.mktemp("dbt_parse"))


@pytest.fixture(scope="session")
def compiled(tmp_path_factory) -> dict:
    """The manifest with every model's compiled SQL. CI's warehouse job compiles against its own
    empty datasets and names the result in JTF_DBT_COMPILED; `make rules` sets JTF_RULES_COMPILE
    to compile here against the settings in .env. Compiling reads no data but needs BigQuery, so
    otherwise (`make test`) the rules over SQL are skipped. Asked for, not guessed from the
    settings: importing dbt loads a .env it finds above the working directory."""
    if path := os.environ.get("JTF_DBT_COMPILED"):
        return json.loads((Path(path) / "manifest.json").read_text())
    if os.environ.get("JTF_RULES_COMPILE") != "1":
        pytest.skip("compiling needs BigQuery: `make rules`, or CI's warehouse job")
    return dbt("compile", tmp_path_factory.mktemp("dbt_compile"))

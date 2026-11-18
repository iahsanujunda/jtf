"""dbt parses the whole project every time, so what a run builds is what is on disk.

A partial parse reuses what it parsed before. Renaming a unit test in the same parse as an edit to
a sources file left the test under both names, and the stale copy, with its old inputs, failed to
compile in every build of its model (2026-11-18, a pull that did both). `partial_parse: false` in
dbt_project.yml turns it off for every dbt call: Dagster's load, each of its runs, `make dbt`.
"""

import json
import os
import shutil
from pathlib import Path

import pytest
import yaml
from dbt.cli.main import dbtRunner

DBT_DIR = Path(__file__).resolve().parents[1]


def parse(project: Path) -> dict:
    """Parse into the copy's own target, where a partial parse would find its cache."""
    target = project / "target"
    args = ["parse", "--quiet", "--project-dir", str(project), "--profiles-dir", str(project)]
    result = dbtRunner().invoke([*args, "--target-path", str(target)])
    assert result.success, result.exception
    return json.loads((target / "manifest.json").read_text())


@pytest.fixture
def project(tmp_path, monkeypatch) -> Path:
    """A copy of the dbt project without its target, so no cache from this machine is reused.
    Parsing reads no data, but the profile names these settings, so they need some value."""
    for name, value in {
        "JTF_BIGQUERY_PROJECT": "test-project",
        "JTF_BIGQUERY_LOCATION": "asia-northeast1",
        "JTF_DBT_DATASET_PREFIX": "test",
    }.items():
        if name not in os.environ:
            monkeypatch.setenv(name, value)
    copy = tmp_path / "dbt"
    shutil.copytree(DBT_DIR, copy, ignore=shutil.ignore_patterns("target", "logs", "__pycache__"))
    return copy


def test_a_renamed_unit_test_is_gone_under_its_old_name(project):
    before = parse(project)
    test = next(iter(before["unit_tests"].values()))
    old, new = test["name"], f"{test['name']}_renamed"
    schema = project / test["original_file_path"]
    text = schema.read_text()
    assert text.count(f"name: {old}\n") == 1
    schema.write_text(text.replace(f"name: {old}\n", f"name: {new}\n"))

    # In the same parse, any edit to a sources file: here a description
    sources = project / next(iter(before["sources"].values()))["original_file_path"]
    content = yaml.safe_load(sources.read_text())
    content["sources"][0]["description"] = "edited"
    sources.write_text(yaml.safe_dump(content, sort_keys=False))

    names = {t["name"] for t in parse(project)["unit_tests"].values()}
    assert new in names
    assert old not in names

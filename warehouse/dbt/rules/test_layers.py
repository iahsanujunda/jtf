"""Layers read only downward, and only DWD reads ODS (rules/README.md, the first rule).

Every model and seed sits in the dataset of its layer and is named for it: `dim_sun_slots` in
`dim`, `dws_capacity` in `dws`. ODS is dbt's source `ods`. DIM holds what nothing is learned about
over time (the sun, the weather points, the parts of the area) and reads only itself, so every
layer can read it. Data tests read any layer: they check, they are not read.
"""

import json
import os
from pathlib import Path

import pytest
from dbt.cli.main import dbtRunner

DBT_DIR = Path(__file__).resolve().parents[1]

READS = {
    "dim": {"dim"},
    "dwd": {"ods", "dim", "dwd"},
    "dws": {"dim", "dwd", "dws"},
    "ads": {"dim", "dwd", "dws", "ads"},
}

# The one read that breaks the rule today, each to go when it is fixed (the last test fails once it
# is no longer there). The decision-time spine reads the newest daily capture from the ODS manifest
# until dwd_source_captures holds it (docs/phase1/progress.md, Next, step 5 item 4).
KNOWN = {("model.jtf.dws_decision_slots", "source.jtf.ods.raw_files")}


@pytest.fixture(scope="module")
def manifest(tmp_path_factory) -> dict:
    """dbt's manifest of the project as it is on disk, parsed afresh into a temporary directory:
    a rule checked against a stale manifest could pass on SQL that no longer exists. Parsing reads
    no data, but the profile names these settings, so they need some value."""
    for name, value in {
        "JTF_BIGQUERY_PROJECT": "test-project",
        "JTF_BIGQUERY_LOCATION": "asia-northeast1",
        "JTF_DBT_DATASET_PREFIX": "test",
    }.items():
        os.environ.setdefault(name, value)
    target = tmp_path_factory.mktemp("dbt_target")
    args = ["parse", "--quiet", "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)]
    result = dbtRunner().invoke([*args, "--target-path", str(target)])
    assert result.success, result.exception
    return json.loads((target / "manifest.json").read_text())


def layer(manifest: dict, unique_id: str) -> str:
    if unique_id in manifest["sources"]:
        return manifest["sources"][unique_id]["source_name"]
    return manifest["nodes"][unique_id]["config"]["schema"]


def built(manifest: dict) -> dict:
    """The models and seeds of this project, by unique id."""
    return {
        uid: node
        for uid, node in manifest["nodes"].items()
        if node["resource_type"] in ("model", "seed") and node["package_name"] == "jtf"
    }


def reads(manifest: dict) -> set[tuple[str, str]]:
    """Every (model, what it reads) pair: refs to models and seeds, and sources."""
    return {
        (uid, dep)
        for uid, node in built(manifest).items()
        if node["resource_type"] == "model"
        for dep in node["depends_on"]["nodes"]
    }


def test_every_model_and_seed_is_in_a_layer_and_named_for_it(manifest):
    wrong = {
        node["name"]: node["config"]["schema"]
        for node in built(manifest).values()
        if node["config"]["schema"] not in READS
        or not node["name"].startswith(f"{node['config']['schema']}_")
    }
    assert not wrong, f"in no layer, or not named for its own: {wrong}"


def test_every_model_reads_only_what_its_layer_may_read(manifest):
    upward = {
        (uid, dep)
        for uid, dep in reads(manifest) - KNOWN
        if layer(manifest, dep) not in READS.get(layer(manifest, uid), set())
    }
    assert not upward, f"reads outside the layers its own may read: {sorted(upward)}"


def test_each_known_exception_is_still_there(manifest):
    gone = KNOWN - reads(manifest)
    assert not gone, f"fixed, so remove from KNOWN: {gone}"

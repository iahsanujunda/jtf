"""The Makefile's settings: which file a target reads, and that a trial build can't land where
main writes. Run through `make` itself, with settings files made here."""

import os
import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]

pytestmark = pytest.mark.skipif(shutil.which("make") is None, reason="needs make")


def settings(path: Path, project: str, prefix: str) -> Path:
    path.write_text(f"JTF_BIGQUERY_PROJECT={project}\nJTF_DBT_DATASET_PREFIX={prefix}\n")
    return path


def make(*args: str, exported: dict[str, str] | None = None) -> subprocess.CompletedProcess:
    env = {k: v for k, v in os.environ.items() if not k.startswith("JTF_")} | (exported or {})
    return subprocess.run(
        ["make", "-s", *args], cwd=REPO, env=env, capture_output=True, text=True, check=False
    )


def test_a_settings_file_wins_over_what_the_shell_already_exports(tmp_path):
    mine = settings(tmp_path / "dev.env", "dev-project", "me")
    # A shell that once loaded main's settings: without the file winning, a build lands there
    shell = {"JTF_BIGQUERY_PROJECT": "main-project", "JTF_DBT_DATASET_PREFIX": "kept"}

    where = make("where", f"ENV_FILE={mine}", exported=shell)

    assert where.returncode == 0 and where.stdout.strip() == "dev-project.me"


def test_a_trial_is_refused_where_main_writes_and_allowed_in_a_place_of_its_own(tmp_path):
    main = settings(tmp_path / "main.env", "main-project", "kept")
    same = settings(tmp_path / "same.env", "main-project", "kept")
    other_prefix = settings(tmp_path / "prefix.env", "main-project", "me")
    other_project = settings(tmp_path / "project.env", "dev-project", "kept")
    shell = {"JTF_BIGQUERY_PROJECT": "main-project", "JTF_DBT_DATASET_PREFIX": "kept"}

    refused = make("own-place", f"MAIN_ENV={main}", f"ENV_FILE={same}")

    assert refused.returncode != 0 and "write where main does (main-project.kept)" in refused.stdout
    # Another file name with the same target is no place of its own; another target is, whatever
    # the shell exports
    for mine in (other_prefix, other_project):
        allowed = make("own-place", f"MAIN_ENV={main}", f"ENV_FILE={mine}", exported=shell)
        assert allowed.returncode == 0, allowed.stdout + allowed.stderr


def test_a_trial_checks_its_place_before_anything_is_built():
    planned = make("-n", "try", "MODELS=x").stdout

    assert "write where main does" in planned
    assert planned.index("write where main does") < planned.index("dbt build")


def test_make_dbt_asks_whether_this_checkout_may_write_before_it_builds():
    planned = make("-n", "dbt").stdout

    assert planned.index("jtf warehouse guard") < planned.index("uv run dbt build")

import re

import pytest
from typer.testing import CliRunner

from jtf import __version__
from jtf.cli.main import app

runner = CliRunner()


class NoClient:
    """Stands in for bigquery.Client where a command must refuse before making one. A class, not
    a function: modules imported after the patch evaluate `bigquery.Client | None`."""

    def __init__(self, *args, **kwargs):
        raise AssertionError("a BigQuery client was made before the arguments were checked")


def said(result) -> str:
    """What a command printed, as a reader sees it: without the colour codes, the box Rich draws
    around an error, or its line breaks. In GitHub Actions, Typer forces Rich's terminal output."""
    text = re.sub(r"\x1b\[[0-9;]*m", "", result.output)
    return " ".join(re.sub(r"[│╭╮╰╯─]", " ", text).split())


def test_help_lists_commands():
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "version" in result.output


def test_version_prints_package_version():
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert result.output.strip() == __version__


def test_parse_needs_both_stores(monkeypatch):
    monkeypatch.delenv("JTF_RAW_URL", raising=False)
    monkeypatch.delenv("JTF_BIGQUERY_PROJECT", raising=False)
    result = runner.invoke(app, ["parse", "--source", "tepco_actuals", "--from", "2026-11-15"])
    assert result.exit_code != 0
    assert "JTF_" in said(result)


def test_ci_refuses_any_prefix_but_a_ci_runs_before_touching_bigquery(monkeypatch):
    monkeypatch.setattr("google.cloud.bigquery.Client", NoClient)
    for command in ("create", "drop"):
        args = ["ci", command, "--project", "p", "--location", "l", "--prefix", "dev"]
        result = runner.invoke(app, args)
        assert result.exit_code == 2
        assert "not a CI run's prefix" in said(result)


SETTINGS = ["--project", "p", "--location", "asia-northeast1", "--prefix", "dev"]


@pytest.mark.parametrize(
    "args",
    [
        [],
        ["--from", "2026-11-17"],
        ["--to", "2026-11-17"],
        ["--from", "2026-11-17", "--reproduce", "r1"],
    ],
    ids=["nothing", "--from alone", "--to alone", "a what-if and a reproduce"],
)
def test_replay_needs_a_range_or_a_run_to_reproduce_before_it_touches_bigquery(args, monkeypatch):
    monkeypatch.setattr("google.cloud.bigquery.Client", NoClient)
    result = runner.invoke(app, ["replay", *SETTINGS, *args])

    assert result.exit_code != 0
    assert "--from and --to for a what-if, or --reproduce" in said(result)


def test_nothing_is_parsed_into_prod_from_a_branch(monkeypatch):
    """This checkout is on a branch, or has changes, whenever tests run before a merge: with the
    settings naming prod, `jtf parse` must refuse before it opens a store or a warehouse."""
    from jtf.orchestration import update

    monkeypatch.setattr(update.Git, "branch", lambda self: "a-branch")
    monkeypatch.setattr("google.cloud.bigquery.Client", NoClient)
    args = ["parse", "--source", "tepco_actuals", "--from", "2026-11-15"]
    env = {"JTF_ENV": "prod", "JTF_RAW_URL": "file:///nowhere", "JTF_BIGQUERY_PROJECT": "p"}

    result = runner.invoke(app, args, env=env)

    assert result.exit_code == 1
    assert "nothing parsed" in said(result) and "not main" in said(result)


def test_dagster_is_not_started_against_prod_with_uncommitted_changes(tmp_path, monkeypatch):
    from jtf.orchestration import update

    monkeypatch.setattr(update.Git, "branch", lambda self: "main")
    monkeypatch.setattr(update.Git, "dirty", lambda self: True)

    refused = runner.invoke(
        app, ["dagster", "ready", "--repo", str(tmp_path)], env={"JTF_ENV": "prod"}
    )

    assert refused.exit_code == 1 and "Dagster not started" in said(refused)
    assert not (tmp_path / ".dagster").exists()

    (tmp_path / "deploy" / "dagster").mkdir(parents=True)
    (tmp_path / "deploy" / "dagster" / "dagster.local.yaml").write_text("retention: {}\n")
    allowed = runner.invoke(
        app, ["dagster", "ready", "--repo", str(tmp_path)], env={"JTF_ENV": "dev"}
    )
    assert allowed.exit_code == 0 and (tmp_path / ".dagster" / "dagster.yaml").exists()


def test_nothing_is_built_in_prod_from_a_branch_or_with_uncommitted_changes(tmp_path, monkeypatch):
    from jtf.orchestration import update

    guard = ["warehouse", "guard", "--repo", str(tmp_path)]
    monkeypatch.setattr(update.Git, "branch", lambda self: "a-change")
    monkeypatch.setattr(update.Git, "dirty", lambda self: False)

    on_a_branch = runner.invoke(app, guard, env={"JTF_ENV": "prod"})

    assert on_a_branch.exit_code == 1
    assert "nothing built" in said(on_a_branch) and "not main" in said(on_a_branch)
    assert runner.invoke(app, guard, env={"JTF_ENV": "dev"}).exit_code == 0  # what dev is for

    monkeypatch.setattr(update.Git, "branch", lambda self: "main")
    assert runner.invoke(app, guard, env={"JTF_ENV": "prod"}).exit_code == 0
    monkeypatch.setattr(update.Git, "dirty", lambda self: True)
    assert runner.invoke(app, guard, env={"JTF_ENV": "prod"}).exit_code == 1

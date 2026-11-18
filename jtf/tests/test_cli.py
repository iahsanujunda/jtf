import pytest
from typer.testing import CliRunner

from jtf import __version__
from jtf.cli.main import app

runner = CliRunner()


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
    assert "JTF_" in result.output


def test_ci_refuses_any_prefix_but_a_ci_runs_before_touching_bigquery(monkeypatch):
    def no_bigquery(*args, **kwargs):
        raise AssertionError("a BigQuery client was made")

    monkeypatch.setattr("google.cloud.bigquery.Client", no_bigquery)
    for command in ("create", "drop"):
        args = ["ci", command, "--project", "p", "--location", "l", "--prefix", "dev"]
        result = runner.invoke(app, args)
        assert result.exit_code == 2
        assert "not a CI run's prefix" in result.output


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
    def no_client(*a, **k):
        raise AssertionError("no BigQuery client before the arguments are checked")

    monkeypatch.setattr("google.cloud.bigquery.Client", no_client)
    result = runner.invoke(app, ["replay", *SETTINGS, *args])

    assert result.exit_code != 0
    assert "--from and --to for a what-if, or --reproduce" in result.output

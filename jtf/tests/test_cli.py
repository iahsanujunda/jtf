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

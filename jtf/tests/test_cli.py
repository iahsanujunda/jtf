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

"""The `jtf` command. Subcommands are thin wrappers over functions in the other subpackages."""

import typer

from jtf import __version__

app = typer.Typer(
    name="jtf",
    help="Day-ahead forecasting pipeline for Tokyo-area solar output and JEPX prices.",
    no_args_is_help=True,
)


@app.callback()
def main() -> None:
    """Every step of the pipeline is a subcommand."""


@app.command()
def version() -> None:
    """Print the installed jtf version."""
    typer.echo(__version__)

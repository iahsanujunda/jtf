"""The `jtf` command. Subcommands are thin wrappers over functions in the other subpackages."""

import logging
import os
from typing import Annotated

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
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


@app.command()
def version() -> None:
    """Print the installed jtf version."""
    typer.echo(__version__)


@app.command()
def sources() -> None:
    """List the capture sources."""
    from jtf.ingest.sources import SOURCES

    for name, source in SOURCES.items():
        typer.echo(f"{name}\t{source.retry.attempts} attempts")


@app.command()
def fetch(
    source: Annotated[str, typer.Option(help="Capture source; see `jtf sources`.")],
    raw_url: Annotated[
        str | None,
        typer.Option(envvar="JTF_RAW_URL", help="Raw store: gs://bucket or file:///path."),
    ] = None,
) -> None:
    """Download one source's files into the raw store, unchanged, with metadata sidecars."""
    from jtf.ingest.fetch import Healthcheck, run_source
    from jtf.ingest.sources import get_source
    from jtf.storage import open_store

    if not raw_url:
        raise typer.BadParameter("Set --raw-url or JTF_RAW_URL.")
    src = get_source(source)
    result = run_source(
        src,
        store=open_store(raw_url),
        healthcheck=Healthcheck.for_source(os.environ.get("JTF_HEALTHCHECK_PING_KEY"), src),
        image_digest=os.environ.get("JTF_IMAGE_DIGEST"),
    )
    typer.echo(result.summary())
    if not result.ok:
        raise typer.Exit(code=1)

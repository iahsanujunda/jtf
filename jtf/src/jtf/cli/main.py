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
    """List the capture sources and the modes each one supports."""
    from jtf.ingest.sources import SOURCES

    for name, source in SOURCES.items():
        typer.echo(f"{name}\t{', '.join(source.modes)}")


@app.command()
def fetch(
    source: Annotated[str, typer.Option(help="Capture source; see `jtf sources`.")],
    raw_url: Annotated[
        str | None,
        typer.Option(envvar="JTF_RAW_URL", help="Raw store: gs://bucket or file:///path."),
    ] = None,
    initial: Annotated[
        bool, typer.Option(help="Load history once; skips what is already stored.")
    ] = False,
    recapture: Annotated[
        bool, typer.Option(help="Check every file for rewrites; stores only what changed.")
    ] = False,
    max_calls: Annotated[
        float | None,
        typer.Option(help="Stop after this many weighted API calls; a later run resumes."),
    ] = None,
) -> None:
    """Download a source's files into the raw store, unchanged, with metadata sidecars."""
    from jtf.ingest.fetch import Healthcheck, run_source
    from jtf.ingest.sources import get_source
    from jtf.storage import open_store

    if not raw_url:
        raise typer.BadParameter("Set --raw-url or JTF_RAW_URL.")
    if initial and recapture:
        raise typer.BadParameter("Choose one of --initial and --recapture.")
    mode = "initial" if initial else "recapture" if recapture else "daily"
    src = get_source(source)
    result = run_source(
        src,
        mode=mode,
        store=open_store(raw_url),
        healthcheck=Healthcheck.for_source(os.environ.get("JTF_HEALTHCHECK_PING_KEY"), src, mode),
        image_digest=os.environ.get("JTF_IMAGE_DIGEST"),
        max_calls=max_calls,
    )
    typer.echo(result.summary())
    if not result.ok:
        raise typer.Exit(code=1)

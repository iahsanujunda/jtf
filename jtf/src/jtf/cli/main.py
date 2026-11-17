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


@app.command()
def parse(
    source: Annotated[str, typer.Option(help="Source with a parser, e.g. tepco_actuals.")],
    from_day: Annotated[str, typer.Option("--from", help="First extraction day, YYYY-MM-DD.")],
    to_day: Annotated[
        str | None, typer.Option("--to", help="Last extraction day; defaults to --from.")
    ] = None,
    table: Annotated[
        str | None,
        typer.Option(help="Only this ODS table, when the source has several layouts."),
    ] = None,
    raw_url: Annotated[
        str | None,
        typer.Option(envvar="JTF_RAW_URL", help="Raw store: gs://bucket or file:///path."),
    ] = None,
    bigquery_project: Annotated[
        str | None,
        typer.Option(envvar="JTF_BIGQUERY_PROJECT", help="Project holding the ods dataset."),
    ] = None,
) -> None:
    """Parse raw files into the source's ODS tables; files already loaded are skipped."""
    import uuid
    from datetime import UTC, date, datetime, timedelta

    from jtf.parse import parsers_for
    from jtf.parse.bigquery import BigQueryWarehouse
    from jtf.parse.load import parse_day
    from jtf.storage import open_store

    if not raw_url or not bigquery_project:
        raise typer.BadParameter("Set --raw-url and --bigquery-project, or their JTF_ variables.")
    parsers = [p for p in parsers_for(source) if table in (None, p.table)]
    if not parsers:
        raise typer.BadParameter(f"{source} has no table {table!r}.")
    store, warehouse = open_store(raw_url), BigQueryWarehouse(bigquery_project)
    first = date.fromisoformat(from_day)
    last = date.fromisoformat(to_day) if to_day else first
    batch_id = f"cli-{uuid.uuid4().hex[:12]}"
    ok = True
    for parser in parsers:
        for offset in range((last - first).days + 1):
            day = (first + timedelta(days=offset)).isoformat()
            result = parse_day(
                parser, store, warehouse, day, batch_id=batch_id, now=datetime.now(UTC)
            )
            typer.echo(f"{parser.table}: {result.summary()}")
            ok = ok and result.ok
    if not ok:
        raise typer.Exit(code=1)


ci = typer.Typer(
    help="A CI run's throwaway datasets: empty ODS and predictions tables; no real data.",
    no_args_is_help=True,
)
app.add_typer(ci, name="ci")

Project = Annotated[str, typer.Option(envvar="JTF_BIGQUERY_PROJECT", help="The dev project.")]
Location = Annotated[str, typer.Option(envvar="JTF_BIGQUERY_LOCATION", help="BigQuery region.")]


def _ci_prefix(prefix: str) -> str:
    """Only a CI run's prefix (ci_<run id>), never dev's or a person's datasets."""
    from jtf.ci import ci_prefix

    try:
        return ci_prefix(prefix)
    except ValueError as e:
        raise typer.BadParameter(str(e), param_hint="--prefix") from e


Prefix = Annotated[
    str,
    typer.Option(
        envvar="JTF_DBT_DATASET_PREFIX", callback=_ci_prefix, help="The run's prefix: ci_<run id>."
    ),
]


@ci.command("create")
def ci_create(project: Project, location: Location, prefix: Prefix) -> None:
    """Make the run's datasets, its empty ODS tables and its empty predictions table."""
    from google.cloud import bigquery

    from jtf.ci import create

    made = create(bigquery.Client(project=project, location=location), project, location, prefix)
    typer.echo(f"{prefix}: {len(made)} empty tables, in datasets whose tables expire in a day")


@ci.command("drop")
def ci_drop(project: Project, location: Location, prefix: Prefix) -> None:
    """Drop every dataset of the run with its tables. Only a CI run's prefix is accepted."""
    from google.cloud import bigquery

    from jtf.ci import drop

    drop(bigquery.Client(project=project, location=location), project, prefix)
    typer.echo(f"{prefix}: dropped")

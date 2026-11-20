"""The `jtf` command. Subcommands are thin wrappers over functions in the other subpackages."""

import logging
import os
from pathlib import Path
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
    import os
    import uuid
    from datetime import UTC, date, datetime, timedelta

    from jtf.orchestration.update import Git, Refused, may_write
    from jtf.parse import parsers_for
    from jtf.parse.bigquery import BigQueryWarehouse
    from jtf.parse.load import parse_day
    from jtf.storage import open_store

    if not raw_url or not bigquery_project:
        raise typer.BadParameter("Set --raw-url and --bigquery-project, or their JTF_ variables.")
    try:
        may_write(Git(Path.cwd()), os.environ)
    except Refused as e:
        typer.echo(f"nothing parsed: {e}", err=True)
        raise typer.Exit(code=1) from e
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


dagster = typer.Typer(help="The laptop's Dagster.", no_args_is_help=True)
app.add_typer(dagster, name="dagster")


@dagster.command("update")
def dagster_update(
    repo: Annotated[Path, typer.Option(help="The checkout Dagster runs from.")] = Path("."),
    url: Annotated[
        str, typer.Option(help="Dagster's GraphQL API.")
    ] = "http://localhost:3000/graphql",
    timeout: Annotated[float, typer.Option(help="Seconds to wait for running runs.")] = 900,
) -> None:
    """Pull main into Dagster's checkout and reload it, with no run in between: stop the sensors,
    wait for running runs, pull, reload, start the sensors again."""
    import subprocess

    from jtf.orchestration.update import Dagster, Git, Refused, update

    def prepare() -> None:
        """The new code's own `jtf warehouse ensure`, in a process that has loaded it."""
        subprocess.run(["uv", "run", "jtf", "warehouse", "ensure"], cwd=repo.resolve(), check=True)

    try:
        typer.echo(
            update(
                Git(repo.resolve()),
                Dagster(url),
                prepare=prepare,
                timeout=timeout,
                echo=typer.echo,
            )
        )
    except Refused as e:
        typer.echo(f"nothing pulled: {e}", err=True)
        raise typer.Exit(code=1) from e
    except RuntimeError as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(code=1) from e


@dagster.command("ready")
def dagster_ready(
    repo: Annotated[Path, typer.Option(help="The checkout Dagster runs from.")] = Path("."),
) -> None:
    """What `make dagster` does before Dagster starts: refuse to start against prod from anything
    but a clean checkout of main, and put Dagster's own settings where it reads them."""
    import os

    from jtf.orchestration.update import Git, Refused, may_write, write_instance_settings

    try:
        may_write(Git(repo.resolve()), os.environ)
    except Refused as e:
        typer.echo(f"Dagster not started: {e}", err=True)
        raise typer.Exit(code=1) from e
    if write_instance_settings(repo.resolve()):
        typer.echo("wrote .dagster/dagster.yaml from deploy/dagster/dagster.local.yaml")


warehouse = typer.Typer(
    help="The warehouse: who may write it, and the tables Python writes.", no_args_is_help=True
)
app.add_typer(warehouse, name="warehouse")


@warehouse.command("guard")
def warehouse_guard(
    repo: Annotated[Path, typer.Option(help="The checkout about to write.")] = Path("."),
) -> None:
    """Refuse unless this checkout may write the warehouse its settings name: prod's tables only
    from a clean checkout of main. What `make dbt` asks before it builds."""
    import os

    from jtf.orchestration.update import Git, Refused, may_write

    try:
        may_write(Git(repo.resolve()), os.environ)
    except Refused as e:
        typer.echo(f"nothing built: {e}", err=True)
        raise typer.Exit(code=1) from e


@warehouse.command("ensure")
def warehouse_ensure(
    project: Project,
    location: Location,
    prefix: Annotated[
        str, typer.Option(envvar="JTF_DBT_DATASET_PREFIX", help="dbt's prefix: <prefix>_ads.")
    ],
) -> None:
    """Make the predictions and runs tables as the code has them, or add the columns they lack."""
    from google.cloud import bigquery

    from jtf.ml.warehouse import ensure_tables

    client = bigquery.Client(project=project, location=location)
    done = ensure_tables(client, project, location, prefix)
    typer.echo(
        "; ".join(done) if done else "the predictions and runs tables are as the code has them"
    )


ods = typer.Typer(
    help="ODS's fingerprints: each file's rows as they were loaded.", no_args_is_help=True
)
app.add_typer(ods, name="ods")
Tables = Annotated[
    list[str] | None,
    typer.Option("--table", help="One ODS table (repeatable); every one if omitted."),
]


def _ods_tables(tables: list[str] | None) -> list[str]:
    from jtf.parse import PARSERS

    unknown = set(tables or []) - set(PARSERS)
    if unknown:
        raise typer.BadParameter(f"not an ODS table: {sorted(unknown)}", param_hint="--table")
    return tables or list(PARSERS)


@ods.command("backfill-fingerprints")
def ods_backfill_fingerprints(project: Project, table: Tables = None) -> None:
    """Fingerprint every file in ODS with no record yet, as `backfill`: guarded from now on, not
    from its load. Reads each table whole; run it with Dagster quiet, as `jtf parse`."""
    from uuid import uuid4

    from jtf.parse.bigquery import BigQueryWarehouse
    from jtf.parse.fingerprint import backfill

    warehouse, batch = BigQueryWarehouse(project), f"backfill-{uuid4()}"
    for name in _ods_tables(table):
        typer.echo(f"{name}: {backfill(warehouse, name, batch_id=batch)} files fingerprinted")


@ods.command("verify")
def ods_verify(
    project: Project,
    table: Tables = None,
    everything: Annotated[
        bool, typer.Option("--all", help="Read every partition, not only those the screen flags.")
    ] = False,
) -> None:
    """Check that every file's rows still match their recorded fingerprint, as the ODS assets do
    after each load; with --all, every file is read again (all of ODS, about 4 GB)."""
    from jtf.parse.bigquery import BigQueryWarehouse
    from jtf.parse.fingerprint import verify

    warehouse, ok = BigQueryWarehouse(project), True
    for name in _ods_tables(table):
        verification = verify(warehouse, name, everything=everything)
        typer.echo(verification.summary())
        for file, problem in verification.problems.items():
            typer.echo(f"  {file}: {problem}")
        ok = ok and verification.ok
    if not ok:
        raise typer.Exit(code=1)


DatasetPrefix = Annotated[
    str, typer.Option(envvar="JTF_DBT_DATASET_PREFIX", help="dbt's datasets: <prefix>_<layer>.")
]


@app.command()
def replay(
    project: Project,
    location: Location,
    prefix: DatasetPrefix,
    first: Annotated[str | None, typer.Option("--from", help="First target day.")] = None,
    last: Annotated[str | None, typer.Option("--to", help="Last target day.")] = None,
    model: Annotated[str, typer.Option(help="The model to run.")] = "persistence_d2",
    original: Annotated[
        str | None, typer.Option("--reproduce", help="An earlier run's ID, run again to match.")
    ] = None,
) -> None:
    """The time machine: a what-if run of a model over target days (--from, --to, YYYY-MM-DD),
    or a reproduce of an earlier run (--reproduce), which must match it exactly. Each run is
    recorded in ads_ml_runs."""
    if bool(original) == bool(first or last) or (not original and not (first and last)):
        raise typer.BadParameter(
            "give --from and --to for a what-if, or --reproduce for a reproduce"
        )
    from datetime import UTC, date, datetime
    from uuid import uuid4

    from jtf.ml.replay import Mismatch, Refused, reproduce, run
    from jtf.ml.tracking import run_code
    from jtf.ml.warehouse import BacktestWarehouse

    warehouse, run_id = BacktestWarehouse(project, location, prefix), f"cli-{uuid4()}"
    now = lambda: datetime.now(UTC)  # noqa: E731
    try:
        if original:
            record = reproduce(
                warehouse, run_id=run_id, original=original, code=run_code(), now=now
            )
        else:
            record = run(
                warehouse,
                run_id=run_id,
                mode="what-if",
                model=model,
                first_day=date.fromisoformat(first),
                last_day=date.fromisoformat(last),
                code=run_code(),
                now=now,
            )
    except (Refused, Mismatch) as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(code=1) from e
    typer.echo(
        f"{record['run_id']}: {record['mode']} of {record['model']}, "
        f"{record['decision_times']} decision times ({record['exact']} exact), "
        f"inputs {record['inputs_fingerprint']}, outputs {record['outputs_fingerprint']}"
    )

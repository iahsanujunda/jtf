"""Throwaway datasets for CI's warehouse check, which reads no real data (phase-1 plan, Decisions,
"Warehouse CI"). Every ODS table the parsers load, and the predictions table models append to, is
created empty from the schemas this code defines for loading them, so the two can't drift apart;
dbt then builds every model on them with `--empty` and runs the unit tests.

Each run has its own prefix, `ci_<run id>[_<attempt>]`, so a cancelled run's cleanup never drops a
newer run's tables, and every table expires after a day in case a run dies before its cleanup.
Only such a prefix can be dropped: never `dev`, never a person's datasets.
"""

import re
from datetime import timedelta

from google.cloud import bigquery

from jtf.ml.warehouse import PREDICTION_SCHEMA, PREDICTIONS
from jtf.parse import PARSERS
from jtf.parse.base import Column
from jtf.parse.bigquery import schema_fields
from jtf.parse.load import MANIFEST, MANIFEST_COLUMNS

LAYERS = ("ods", "dim", "dwd", "dws", "ads")
EXPIRY = timedelta(days=1)
LABELS = {"purpose": "ci"}
PREFIX = re.compile(r"ci_\d+(_\d+)?")


def ci_prefix(prefix: str) -> str:
    """The prefix, if it names a CI run's datasets; anything else is refused."""
    if not PREFIX.fullmatch(prefix):
        raise ValueError(f"not a CI run's prefix: {prefix!r}; expected ci_<run id>[_<attempt>]")
    return prefix


def ods_tables() -> dict[str, tuple[tuple[Column, ...], str]]:
    """Every ODS table with its columns and clustering column, as the loader makes them: one per
    file layout, and the manifest."""
    tables = {parser.table: (parser.schema, "source_file") for parser in PARSERS.values()}
    tables[MANIFEST] = (MANIFEST_COLUMNS, "source")
    return tables


def create(client: bigquery.Client, project: str, location: str, prefix: str) -> list[str]:
    """The run's datasets, one per layer, and its empty ODS and predictions tables. Returns the
    tables made."""
    prefix = ci_prefix(prefix)
    for layer in LAYERS:
        dataset = bigquery.Dataset(f"{project}.{prefix}_{layer}")
        dataset.location = location
        dataset.default_table_expiration_ms = int(EXPIRY.total_seconds() * 1000)
        dataset.labels = LABELS
        client.create_dataset(dataset, exists_ok=True)
    made = []
    for name, (columns, cluster) in ods_tables().items():
        table = bigquery.Table(f"{project}.{prefix}_ods.{name}", schema=schema_fields(columns))
        table.time_partitioning = bigquery.TimePartitioning(field="extraction_date")
        table.clustering_fields = [cluster]
        made.append(client.create_table(table, exists_ok=True).table_id)
    predictions = bigquery.Table(f"{project}.{prefix}_ads.{PREDICTIONS}", schema=PREDICTION_SCHEMA)
    predictions.clustering_fields = ["run_id"]
    made.append(client.create_table(predictions, exists_ok=True).table_id)
    return made


def drop(client: bigquery.Client, project: str, prefix: str) -> None:
    """Every dataset of the run, with its tables."""
    prefix = ci_prefix(prefix)
    for layer in LAYERS:
        dataset = f"{project}.{prefix}_{layer}"
        client.delete_dataset(dataset, delete_contents=True, not_found_ok=True)

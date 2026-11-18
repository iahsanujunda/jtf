"""The ODS dataset in BigQuery. Tables are created by their first append-only load."""

from __future__ import annotations

from datetime import date

import pandas as pd
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from jtf.parse.base import Column
from jtf.parse.fingerprint import FINGERPRINT_COLUMNS, FINGERPRINT_SQL, FINGERPRINTS
from jtf.parse.load import MANIFEST


def schema_fields(columns: tuple[Column, ...]) -> list[bigquery.SchemaField]:
    """A table's columns as BigQuery takes them: the loader's and CI's empty tables use the same."""
    return [
        bigquery.SchemaField(c.name, c.type, mode="REQUIRED" if c.required else "NULLABLE")
        for c in columns
    ]


class BigQueryWarehouse:
    def __init__(self, project: str, dataset: str = "ods", client: bigquery.Client | None = None):
        self.client = client or bigquery.Client(project=project)
        self.dataset = f"{project}.{dataset}"

    def manifest_files(self, table: str, version: str, day: date) -> set[str]:
        return self._files(
            f"SELECT DISTINCT path FROM `{self.dataset}.{MANIFEST}` "
            "WHERE extraction_date = @day AND ods_table = @table "
            "AND parser_version = @version AND status != 'failed'",
            day=("DATE", day),
            version=("STRING", version),
            table=("STRING", table),
        )

    def table_files(self, table: str, version: str, day: date) -> set[str]:
        return self._files(
            f"SELECT DISTINCT source_file FROM `{self.dataset}.{table}` "
            "WHERE extraction_date = @day AND parser_version = @version",
            day=("DATE", day),
            version=("STRING", version),
        )

    def append(
        self, table: str, schema: tuple[Column, ...], cluster_by: str, rows: pd.DataFrame
    ) -> None:
        config = bigquery.LoadJobConfig(
            schema=schema_fields(schema),
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            time_partitioning=bigquery.TimePartitioning(field="extraction_date"),
            clustering_fields=[cluster_by],
        )
        job = self.client.load_table_from_dataframe(
            rows, f"{self.dataset}.{table}", job_config=config
        )
        job.result()

    def fingerprints(
        self, table: str, days: list[date] | None, files: list[str] | None = None
    ) -> pd.DataFrame:
        """Fingerprints as they are now, `recorded_at` BigQuery's time, so a partition's
        `last_modified_time` compares with it on one clock. Days and files are query
        parameters, which BigQuery prunes partitions by, as it does constants."""
        where, params = [], []
        if days is not None:
            where.append("extraction_date IN UNNEST(@days)")
            params.append(bigquery.ArrayQueryParameter("days", "DATE", days))
        if files is not None:
            where.append("source_file IN UNNEST(@files)")
            params.append(bigquery.ArrayQueryParameter("files", "STRING", files))
        sql = (
            "SELECT source_file, parser_version, extraction_date, COUNT(*) AS row_count, "
            f"{FINGERPRINT_SQL} AS fingerprint, CURRENT_TIMESTAMP() AS recorded_at "
            f"FROM `{self.dataset}.{table}` AS t "
            + (f"WHERE {' AND '.join(where)} " if where else "")
            + "GROUP BY source_file, parser_version, extraction_date"
        )
        return self._frame(sql, params)

    def partitions(self, table: str) -> pd.DataFrame:
        """Free metadata, from INFORMATION_SCHEMA.PARTITIONS."""
        sql = (
            "SELECT PARSE_DATE('%Y%m%d', partition_id) AS extraction_date, total_rows, "
            "last_modified_time AS last_modified "
            f"FROM `{self.dataset}.INFORMATION_SCHEMA.PARTITIONS` "
            "WHERE table_name = @table AND partition_id NOT IN ('__NULL__', '__UNPARTITIONED__')"
        )
        return self._frame(sql, [bigquery.ScalarQueryParameter("table", "STRING", table)])

    def recorded(self, table: str) -> pd.DataFrame:
        sql = f"SELECT * FROM `{self.dataset}.{FINGERPRINTS}` WHERE ods_table = @table"
        rows = self._frame(sql, [bigquery.ScalarQueryParameter("table", "STRING", table)])
        return (
            rows if not rows.empty else pd.DataFrame(columns=[c.name for c in FINGERPRINT_COLUMNS])
        )

    def _frame(self, sql: str, params: list) -> pd.DataFrame:
        config = bigquery.QueryJobConfig(query_parameters=params)
        try:
            job = self.client.query(sql, job_config=config)
            return job.to_dataframe(create_bqstorage_client=False)
        except NotFound:  # the table is created by its first load
            return pd.DataFrame()

    def _files(self, sql: str, **params: tuple[str, object]) -> set[str]:
        """Run a query whose single column is a file path; `params` maps name => (type, value)."""
        query_params = [bigquery.ScalarQueryParameter(n, t, v) for n, (t, v) in params.items()]
        config = bigquery.QueryJobConfig(query_parameters=query_params)
        try:
            job = self.client.query(sql, job_config=config)
            return {row[0] for row in job.result()}
        except NotFound:  # the table is created by its first load
            return set()

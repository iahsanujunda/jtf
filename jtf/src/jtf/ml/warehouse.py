"""dbt's datasets, `<prefix>_<layer>`, as a model's runs read and write them: features and
fidelity from DWS, labels and outcomes from ADS, and predictions and run records appended to ADS,
where dbt reads the predictions as a source."""

from datetime import date

import pandas as pd
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from jtf.parse.fingerprint import ROW_JSON

PREDICTIONS = "ads_ml_prediction_solar"
RUNS = "ads_ml_runs"
# What every run reads of the features beside its model's own columns, and the part of its
# predictions a reproduce must match: not its run ID or mode, which differ by design
KEY_COLUMNS = ["decision_time", "target_day", "slot_start"]
OUTPUT_COLUMNS = [
    "model",
    "decision_time",
    "target_day",
    "slot_start",
    "predicted_mw",
    "p10_mw",
    "p90_mw",
]

PREDICTION_SCHEMA = [
    bigquery.SchemaField("run_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("model", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("mode", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("fold", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("decision_time", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("target_day", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("slot_start", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("predicted_mw", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("p10_mw", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("p90_mw", "FLOAT", mode="NULLABLE"),
]
# A table made before a column was added gains it at the next append
GROWS = [bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]


RUN_SCHEMA = [
    bigquery.SchemaField("run_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("mode", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("reproduces", "STRING"),
    bigquery.SchemaField("model", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("model_version", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("git_commit", "STRING"),
    bigquery.SchemaField("tree_dirty", "BOOLEAN"),
    bigquery.SchemaField("image_digest", "STRING"),
    bigquery.SchemaField("first_decision_time", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("last_decision_time", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("decision_times", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("exact", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("approximate", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("fault", "STRING"),
    bigquery.SchemaField("inputs_fingerprint", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("outputs_fingerprint", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("training_fingerprint", "STRING"),
    bigquery.SchemaField("started_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("finished_at", "TIMESTAMP", mode="REQUIRED"),
]


def fingerprint_of(columns: list[str]) -> str:
    """The ODS check's fingerprint, over the named columns of each row of `t`."""
    row = ROW_JSON.replace("TO_JSON(t)", f"TO_JSON(STRUCT({', '.join(f't.{c}' for c in columns)}))")
    return f"CAST(SUM(CAST(FARM_FINGERPRINT({row}) AS BIGNUMERIC)) AS STRING)"


class BacktestWarehouse:
    def __init__(
        self, project: str, location: str, prefix: str, client: bigquery.Client | None = None
    ):
        self.client = client or bigquery.Client(project=project, location=location)
        self.location = location
        self.datasets = f"{project}.{prefix}"

    def table(self, layer: str, name: str) -> str:
        return f"{self.datasets}_{layer}.{name}"

    def newest_complete_label_day(self) -> date | None:
        """The newest day (JST) with a label in all 48 slots."""
        sql = f"""
            select max(day) as day
            from (
                select date(slot_start, 'Asia/Tokyo') as day
                from `{self.table("ads", "ads_ml_label_solar")}`
                where label_mw is not null
                group by day
                having count(*) = 48
            )
        """
        day = self._query(sql)["day"].iloc[0]
        return None if pd.isna(day) else day

    def features(self, first_day: date, last_day: date, columns: tuple[str, ...]) -> pd.DataFrame:
        """The keys and `columns` of dws_solar_features for the target days `first_day` to
        `last_day`, each row with its fold (the target day's month) and whether its slot is in
        daylight (dim_daylight: the sun's place, known beforehand), sorted so that the same days
        always give the same frame."""
        sql = f"""
            select date_trunc(f.target_day, month) as fold,
                {", ".join(f"f.{c}" for c in [*KEY_COLUMNS, *columns])},
                not d.is_night as is_daylight
            from `{self.table("dws", "dws_solar_features")}` as f
            left join `{self.table("dim", "dim_daylight")}` as d using (slot_start)
            where f.target_day between @first and @last
            order by f.decision_time, f.slot_start
        """
        return self._query(sql, first=("DATE", first_day), last=("DATE", last_day))

    def inputs_fingerprint(self, first_day: date, last_day: date, columns: tuple[str, ...]) -> str:
        """The features a run of these target days reads, fingerprinted: its model's columns and
        no others, so a feature it doesn't read can change without the run looking different."""
        sql = f"""
            select {fingerprint_of([*KEY_COLUMNS, *columns])} as fingerprint
            from `{self.table("dws", "dws_solar_features")}` as t
            where target_day between @first and @last
        """
        return self._query(sql, first=("DATE", first_day), last=("DATE", last_day))[
            "fingerprint"
        ].iloc[0]

    def _training_rows(self, columns: tuple[str, ...]) -> str:
        """Every labelled slot with its keys, `columns`, label and daylight flag, as a query."""
        return f"""
            select {", ".join(f"f.{c}" for c in [*KEY_COLUMNS, *columns])}, l.label_mw,
                not d.is_night as is_daylight
            from `{self.table("dws", "dws_solar_features")}` as f
            inner join `{self.table("ads", "ads_ml_label_solar")}` as l using (slot_start)
            inner join `{self.table("dim", "dim_daylight")}` as d using (slot_start)
            where f.target_day <= @last and l.label_mw is not null
        """

    def training(self, last_day: date, columns: tuple[str, ...]) -> pd.DataFrame:
        """What a model may be trained on: every labelled slot of the target days up to
        `last_day`, sorted so that the same days always give the same frame."""
        sql = f"{self._training_rows(columns)} order by decision_time, slot_start"
        return self._query(sql, last=("DATE", last_day))

    def training_fingerprints(
        self, last_days: list[date], columns: tuple[str, ...]
    ) -> dict[date, str]:
        """Per last day, the rows `training` gives up to it, fingerprinted, labels included: a
        label revised later shows as changed, in every training that learned from it."""
        names = [*KEY_COLUMNS, *columns, "label_mw", "is_daylight"]
        sql = f"""
            select last_day, {fingerprint_of(names)} as fingerprint
            from unnest(@last_days) as last_day
            inner join ({self._training_rows(columns)}) as t on t.target_day <= last_day
            group by last_day
        """
        config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("last", "DATE", max(last_days)),
                bigquery.ArrayQueryParameter("last_days", "DATE", sorted(set(last_days))),
            ]
        )
        found = self.client.query(sql, job_config=config).to_dataframe()
        return dict(zip(found["last_day"], found["fingerprint"], strict=True))

    def outputs_fingerprint(self, run_id: str) -> str:
        """A run's predictions fingerprinted, without its run ID and mode."""
        sql = f"""
            select {fingerprint_of(OUTPUT_COLUMNS)} as fingerprint
            from `{self.table("ads", PREDICTIONS)}` as t
            where run_id = @run_id
        """
        return self._query(sql, run_id=("STRING", run_id))["fingerprint"].iloc[0]

    def fidelity(self, first_day: date, last_day: date, inputs: tuple[str, ...]) -> pd.Series:
        """Per decision time of the target days, whether every one of `inputs` is exact."""
        sql = f"""
            select decision_time, logical_and(fidelity = 'exact') as exact
            from `{self.table("dws", "dws_fidelity")}`
            where target_day between @first and @last and input in unnest(@inputs)
            group by decision_time
            order by decision_time
        """
        config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("first", "DATE", first_day),
                bigquery.ScalarQueryParameter("last", "DATE", last_day),
                bigquery.ArrayQueryParameter("inputs", "STRING", list(inputs)),
            ]
        )
        found = self.client.query(sql, job_config=config).to_dataframe()
        return found.set_index("decision_time")["exact"].astype(bool)

    def append_run(self, record: pd.DataFrame) -> None:
        """Append one run's record; ads_ml_runs is append-only, like the predictions."""
        config = bigquery.LoadJobConfig(
            schema=RUN_SCHEMA,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema_update_options=GROWS,
        )
        self.client.load_table_from_dataframe(
            record, self.table("ads", RUNS), job_config=config
        ).result()

    def run(self, run_id: str) -> pd.DataFrame:
        """One run's record, empty if there is none."""
        sql = f"select * from `{self.table('ads', RUNS)}` where run_id = @run_id"
        try:
            return self._query(sql, run_id=("STRING", run_id))
        except NotFound:
            return pd.DataFrame()

    def append_predictions(self, rows: pd.DataFrame) -> None:
        """Append one run's predictions; earlier runs stay, each under its own run_id."""
        dataset = bigquery.Dataset(self.datasets + "_ads")
        dataset.location = self.location
        self.client.create_dataset(dataset, exists_ok=True)
        config = bigquery.LoadJobConfig(
            schema=PREDICTION_SCHEMA,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema_update_options=GROWS,
            clustering_fields=["run_id"],
        )
        table = self.table("ads", PREDICTIONS)
        self.client.load_table_from_dataframe(rows, table, job_config=config).result()

    def outcomes(self, run_id: str) -> pd.DataFrame:
        """One run's rows of ads_ml_outcome_solar, sorted, so its metrics add up the same each
        time."""
        sql = f"""
            select fold, decision_time, target_day, slot_start, error_mw, error_share,
                is_daylight, in_range, pinball_share, sky, is_curtailed_day, season,
                price_yen_kwh
            from `{self.table("ads", "ads_ml_outcome_solar")}`
            where run_id = @run_id
            order by fold, decision_time, slot_start
        """
        return self._query(sql, run_id=("STRING", run_id))

    def benchmark(self, run_id: str) -> pd.DataFrame:
        """One run's rows of ads_ml_benchmark_solar, its forecasts beside TEPCO's, sorted."""
        sql = f"""
            select decision_time, target_day, slot_start, error_mw, error_share,
                tepco_error_mw, tepco_error_share, is_daylight
            from `{self.table("ads", "ads_ml_benchmark_solar")}`
            where run_id = @run_id
            order by decision_time, slot_start
        """
        return self._query(sql, run_id=("STRING", run_id))

    def _query(self, sql: str, **params: tuple[str, object]) -> pd.DataFrame:
        """`params` maps name => (type, value)."""
        query_params = [bigquery.ScalarQueryParameter(n, t, v) for n, (t, v) in params.items()]
        config = bigquery.QueryJobConfig(query_parameters=query_params)
        return self.client.query(sql, job_config=config).to_dataframe()

"""dbt's datasets, `<prefix>_<layer>`, as the backtest reads and writes them: features from DWS,
labels and outcomes from ADS, and predictions appended to ADS, where dbt reads them as a source."""

from datetime import date

import pandas as pd
from google.cloud import bigquery

PREDICTIONS = "ads_prediction_solar"

PREDICTION_SCHEMA = [
    bigquery.SchemaField("run_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("model", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("mode", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("fold", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("decision_time", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("target_day", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("slot_start", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("predicted_mw", "FLOAT", mode="NULLABLE"),
]


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
                from `{self.table("ads", "ads_label_solar")}`
                where label_mw is not null
                group by day
                having count(*) = 48
            )
        """
        day = self._query(sql)["day"].iloc[0]
        return None if pd.isna(day) else day

    def features(self, first: date, last: date) -> pd.DataFrame:
        """dws_solar_features for every target day of the months `first` to `last`, each row with
        its fold, sorted so that the same months always give the same frame."""
        sql = f"""
            select
                date_trunc(target_day, month) as fold,
                decision_time,
                target_day,
                slot_start,
                solar_d2_mw
            from `{self.table("dws", "dws_solar_features")}`
            where target_day >= @first and target_day < date_add(@last, interval 1 month)
            order by decision_time, slot_start
        """
        return self._query(sql, first=("DATE", first), last=("DATE", last))

    def append_predictions(self, rows: pd.DataFrame) -> None:
        """Append one run's predictions; earlier runs stay, each under its own run_id."""
        dataset = bigquery.Dataset(self.datasets + "_ads")
        dataset.location = self.location
        self.client.create_dataset(dataset, exists_ok=True)
        config = bigquery.LoadJobConfig(
            schema=PREDICTION_SCHEMA,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            clustering_fields=["run_id"],
        )
        table = self.table("ads", PREDICTIONS)
        self.client.load_table_from_dataframe(rows, table, job_config=config).result()

    def outcomes(self, run_id: str) -> pd.DataFrame:
        """One run's rows of ads_outcome_solar, sorted, so its metrics add up the same each time."""
        sql = f"""
            select fold, decision_time, slot_start, error_mw, error_share, is_daylight
            from `{self.table("ads", "ads_outcome_solar")}`
            where run_id = @run_id
            order by fold, decision_time, slot_start
        """
        return self._query(sql, run_id=("STRING", run_id))

    def _query(self, sql: str, **params: tuple[str, object]) -> pd.DataFrame:
        """`params` maps name => (type, value)."""
        query_params = [bigquery.ScalarQueryParameter(n, t, v) for n, (t, v) in params.items()]
        config = bigquery.QueryJobConfig(query_parameters=query_params)
        return self.client.query(sql, job_config=config).to_dataframe()

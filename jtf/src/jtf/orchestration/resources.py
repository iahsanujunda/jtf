import dagster as dg

from jtf.ml.warehouse import BacktestWarehouse
from jtf.parse.bigquery import BigQueryWarehouse
from jtf.parse.load import Warehouse
from jtf.storage import Store, open_store


class RawBucket(dg.ConfigurableResource):
    """The raw bucket. Dagster lists and reads captures; only the capture jobs write them."""

    url: str

    def store(self) -> Store:
        return open_store(self.url)


class BigQuery(dg.ConfigurableResource):
    """The project holding the ods dataset: dev from the laptop, prod in prod."""

    project: str

    def warehouse(self) -> Warehouse:
        return BigQueryWarehouse(self.project)


class Datasets(dg.ConfigurableResource):
    """dbt's datasets, `<prefix>_<layer>`: model runs read features and labels there and append
    their predictions to ADS."""

    project: str
    location: str
    prefix: str

    def backtest(self) -> BacktestWarehouse:
        return BacktestWarehouse(self.project, self.location, self.prefix)

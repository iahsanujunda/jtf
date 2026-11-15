import dagster as dg

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

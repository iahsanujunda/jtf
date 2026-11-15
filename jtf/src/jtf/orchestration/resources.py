import dagster as dg

from jtf.storage import Store, open_store


class RawBucket(dg.ConfigurableResource):
    """The raw bucket. Dagster lists and reads captures; only the capture jobs write them."""

    url: str

    def store(self) -> Store:
        return open_store(self.url)

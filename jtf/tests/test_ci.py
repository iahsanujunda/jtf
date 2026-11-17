from pathlib import Path

import pytest
import yaml

from jtf.ci import EXPIRY, LAYERS, create, drop, ods_tables
from jtf.ml.warehouse import PREDICTIONS
from jtf.parse import PARSERS

MODELS = Path(__file__).parents[2] / "warehouse/dbt/models"


class FakeClient:
    def __init__(self):
        self.datasets, self.tables, self.deleted = {}, {}, []

    def create_dataset(self, dataset, exists_ok):
        self.datasets[dataset.dataset_id] = dataset

    def create_table(self, table, exists_ok):
        self.tables[(table.dataset_id, table.table_id)] = table
        return table

    def delete_dataset(self, dataset, delete_contents, not_found_ok):
        assert delete_contents and not_found_ok
        self.deleted.append(dataset)


def dbt_sources(name: str) -> set[str]:
    """The tables dbt reads from outside dbt under one source, as its yml declares them."""
    declared = [
        source
        for path in MODELS.glob("*/_*__sources.yml")
        for source in yaml.safe_load(path.read_text())["sources"]
    ]
    return {table["name"] for s in declared if s["name"] == name for table in s["tables"]}


def test_every_table_dbt_reads_from_outside_dbt_is_made_empty_as_the_loader_makes_it():
    client = FakeClient()
    create(client, "p", "asia-northeast1", "ci_123_1")
    ods = {name: t for (dataset, name), t in client.tables.items() if dataset == "ci_123_1_ods"}
    ads = {name for (dataset, name) in client.tables if dataset == "ci_123_1_ads"}
    # A source dbt reads that CI didn't make would fail the CI build; this fails first, offline.
    assert dbt_sources("ods") <= set(ods)
    assert dbt_sources("ads") <= ads == {PREDICTIONS}
    assert set(ods) == set(ods_tables())
    for parser in PARSERS.values():
        table = ods[parser.table]
        assert [f.name for f in table.schema] == [c.name for c in parser.schema]
        assert table.time_partitioning.field == "extraction_date"
        assert table.clustering_fields == ["source_file"]
    assert ods["raw_files"].clustering_fields == ["source"]


def test_every_layer_gets_a_dataset_whose_tables_expire_in_a_day():
    client = FakeClient()
    create(client, "p", "asia-northeast1", "ci_123_1")
    assert set(client.datasets) == {f"ci_123_1_{layer}" for layer in LAYERS}
    for dataset in client.datasets.values():
        assert dataset.default_table_expiration_ms == EXPIRY.total_seconds() * 1000 == 86_400_000
        assert dataset.location == "asia-northeast1"
        assert dataset.labels == {"purpose": "ci"}


def test_drop_removes_every_layer_of_the_run():
    client = FakeClient()
    drop(client, "p", "ci_123")
    assert client.deleted == [f"p.ci_123_{layer}" for layer in LAYERS]


@pytest.mark.parametrize("prefix", ["dev", "pr_15", "ci_", "ci_abc", "ci_123_dev", "dev_ci_123"])
def test_only_a_ci_runs_prefix_is_ever_created_or_dropped(prefix):
    client = FakeClient()
    with pytest.raises(ValueError, match="not a CI run's prefix"):
        drop(client, "p", prefix)
    with pytest.raises(ValueError, match="not a CI run's prefix"):
        create(client, "p", "asia-northeast1", prefix)
    assert client.deleted == [] and client.datasets == {}

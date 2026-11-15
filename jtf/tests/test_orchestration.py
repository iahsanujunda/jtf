import json

import dagster as dg

from conftest import FIXTURES
from jtf.ingest.sources import SOURCES
from jtf.orchestration.definitions import defs
from jtf.orchestration.ods import FILE_CHECKS, ODS_ASSETS
from jtf.orchestration.raw import RAW_ASSETS, raw_bucket_sensor, raw_key
from jtf.orchestration.resources import BigQuery, RawBucket
from jtf.storage import LocalStore


def test_definitions_load():
    dg.Definitions.validate_loadable(defs)


def test_every_source_has_a_raw_asset_and_scheduled_ones_a_freshness_policy():
    specs = {spec.key: spec for spec in RAW_ASSETS}
    assert set(specs) == {raw_key(name) for name in SOURCES}
    for source in SOURCES.values():
        has_policy = specs[raw_key(source.name)].freshness_policy is not None
        assert has_policy == (source.items is not None), source.name


def test_sensor_records_new_files_once_per_source_and_day(tmp_path):
    store = LocalStore(tmp_path)
    for path in (
        "tepco_actuals/2026-11-15/20261115T020416Z_eria_jukyu_202611_03.csv",
        "tepco_actuals/2026-11-15/20261115T045240Z_area-2016.csv",
        "jepx_spot/2026-11-15/20261115T020812Z_spot_summary_2026.csv",
        "tepco_sameday/2026-11-15/20261115T020417Z_AREA_JISEKI.csv",  # not a registered source
    ):
        store.put_new(path, b"data", "text/csv")
        store.put_new(path + ".meta.json", b"{}", "application/json")
    resources = {"raw_bucket": RawBucket(url=f"file://{tmp_path}")}

    first = raw_bucket_sensor(dg.build_sensor_context(resources=resources))

    recorded = {(e.asset_key, e.partition): e.metadata["files"].value for e in first.asset_events}
    assert recorded == {
        (raw_key("tepco_actuals"), "2026-11-15"): 2,
        (raw_key("jepx_spot"), "2026-11-15"): 1,
    }
    assert set(json.loads(first.cursor)) == {"tepco_actuals", "jepx_spot"}

    again = raw_bucket_sensor(dg.build_sensor_context(cursor=first.cursor, resources=resources))
    assert again.asset_events == []


class FakeBigQuery(BigQuery):
    def warehouse(self):
        return WAREHOUSE[0]


WAREHOUSE: list = []


def test_the_ods_asset_loads_a_day_and_reports_its_checks(warehouse):
    WAREHOUSE[:] = [warehouse]
    (ods_tepco_actuals,) = ODS_ASSETS
    resources = {
        "raw_bucket": RawBucket(url=FIXTURES.as_uri()),
        "bigquery": FakeBigQuery(project="test"),
    }

    result = dg.materialize([ods_tepco_actuals], partition_key="2026-11-15", resources=resources)

    assert result.success
    (materialization,) = result.asset_materializations_for_node(ods_tepco_actuals.node_def.name)
    assert materialization.metadata["rows"].value == 202
    (check,) = result.get_asset_check_evaluations()
    assert check.check_name == FILE_CHECKS and check.passed
    assert len(warehouse.rows("tepco_actuals")) == 202


def test_automation_runs_by_default():
    sensor = defs.resolve_sensor_def("automation")
    assert sensor.default_status == dg.DefaultSensorStatus.RUNNING

import json

import dagster as dg

from conftest import FIXTURES
from jtf.ingest.landed import SIDECAR
from jtf.ingest.sources import SOURCES
from jtf.orchestration.definitions import defs
from jtf.orchestration.ods import FILE_CHECKS, ODS_ASSETS, ods_key
from jtf.orchestration.raw import RAW_ASSETS, raw_bucket_sensor, raw_key
from jtf.orchestration.resources import BigQuery, RawBucket
from jtf.parse import PARSERS
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
FIXTURE_RESOURCES = {
    "raw_bucket": RawBucket(url=FIXTURES.as_uri()),
    "bigquery": FakeBigQuery(project="test"),
}


def test_the_ods_asset_loads_a_day_and_reports_its_checks(warehouse):
    WAREHOUSE[:] = [warehouse]
    (ods_tepco_actuals,) = [a for a in ODS_ASSETS if a.key == ods_key(PARSERS["tepco_actuals"])]

    result = dg.materialize(
        [ods_tepco_actuals], partition_key="2026-11-15", resources=FIXTURE_RESOURCES
    )

    assert result.success
    (materialization,) = result.asset_materializations_for_node(ods_tepco_actuals.node_def.name)
    assert materialization.metadata["rows"].value == 202
    (check,) = result.get_asset_check_evaluations()
    assert check.check_name == FILE_CHECKS and check.passed
    assert len(warehouse.rows("tepco_actuals")) == 202


def test_every_file_of_a_day_is_parsed_by_some_ods_asset(warehouse):
    # Not exactly one: a demand outlook file holds three shapes, one table each.
    WAREHOUSE[:] = [warehouse]

    result = dg.materialize(ODS_ASSETS, partition_key="2026-11-15", resources=FIXTURE_RESOURCES)

    assert result.success
    landed = {
        path.relative_to(FIXTURES).as_posix()
        for path in FIXTURES.glob("*/2026-11-15/*")
        if not path.name.endswith(SIDECAR)
    }
    assert set(warehouse.rows("raw_files")["path"]) == landed


def test_automation_runs_by_default():
    sensor = defs.resolve_sensor_def("automation")
    assert sensor.default_status == dg.DefaultSensorStatus.RUNNING

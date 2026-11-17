import json

import dagster as dg

from conftest import FIXTURES
from jtf.ingest.landed import SIDECAR
from jtf.ingest.sources import SOURCES
from jtf.orchestration.dbt import DBT_PROJECT, REBUILD_ON_UPDATE, dbt_models
from jtf.orchestration.definitions import defs
from jtf.orchestration.ods import FILE_CHECKS, MANIFEST_KEY, MANIFEST_SPEC, ODS_ASSETS, ods_key
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


def test_the_ods_asset_loads_a_day_and_reports_its_checks_and_the_manifest(warehouse):
    WAREHOUSE[:] = [warehouse]
    (ods_tepco_actuals,) = [a for a in ODS_ASSETS if a.key == ods_key(PARSERS["tepco_actuals"])]
    node = ods_tepco_actuals.node_def.name

    result = dg.materialize(
        [ods_tepco_actuals], partition_key="2026-11-15", resources=FIXTURE_RESOURCES
    )

    assert result.success
    reported = {m.asset_key: m for m in result.asset_materializations_for_node(node)}
    assert set(reported) == {ods_tepco_actuals.key, MANIFEST_KEY}
    assert reported[ods_tepco_actuals.key].metadata["rows"].value == 202
    (check,) = result.get_asset_check_evaluations()
    assert check.check_name == FILE_CHECKS and check.passed
    assert len(warehouse.rows("tepco_actuals")) == 202

    again = dg.materialize(
        [ods_tepco_actuals], partition_key="2026-11-15", resources=FIXTURE_RESOURCES
    )

    # Every file skipped: the manifest is untouched, so nothing downstream should rebuild.
    assert {m.asset_key for m in again.asset_materializations_for_node(node)} == {
        ods_tepco_actuals.key
    }


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


def test_dbt_reads_only_ods_assets_so_lineage_runs_from_raw_files():
    produced = set(dbt_models.keys)
    read = {dep.asset_key for spec in dbt_models.specs for dep in spec.deps} - produced
    assert read <= {a.key for a in ODS_ASSETS} | {MANIFEST_KEY}


def test_models_rebuild_on_their_own_and_seeds_only_by_hand():
    nodes = json.loads(DBT_PROJECT.manifest_path.read_text())["nodes"].values()
    seeds = {n["name"] for n in nodes if n["resource_type"] == "seed"}
    for spec in dbt_models.specs:
        assert spec.key.path[0] == spec.group_name in {"dwd", "dws"}, spec.key
        expected = None if spec.key.path[-1] in seeds else REBUILD_ON_UPDATE
        assert spec.automation_condition == expected, spec.key


def test_the_manifest_is_downstream_of_every_ods_asset():
    assert {dep.asset_key for dep in MANIFEST_SPEC.deps} == {a.key for a in ODS_ASSETS}


class StandIns:
    """Two ODS assets, the manifest, a DWD model and the spine, with the real automation condition.

    Like the real ones, the ODS stand-ins report the manifest from their own step, and one run
    parses both: `other` first, then `actuals`, which the model reads. `during_actuals` runs
    inside the `actuals` step, while the run is still going.
    """

    def __init__(self):
        days = dg.DailyPartitionsDefinition(start_date="2026-01-01", timezone="Asia/Tokyo")
        other_key, actuals_key = dg.AssetKey(["ods", "other"]), dg.AssetKey(["ods", "actuals"])
        self.model_key = dg.AssetKey(["dwd", "model"])
        self.spine_key = dg.AssetKey(["dws", "spine"])
        self.during_actuals = lambda: None

        @dg.asset(key=other_key, partitions_def=days)
        def other(context: dg.AssetExecutionContext):
            context.log_event(dg.AssetMaterialization(asset_key=MANIFEST_KEY))

        @dg.asset(key=actuals_key, deps=[other_key], partitions_def=days)
        def actuals(context: dg.AssetExecutionContext):
            self.during_actuals()
            context.log_event(dg.AssetMaterialization(asset_key=MANIFEST_KEY))

        manifest = dg.AssetSpec(MANIFEST_KEY, deps=[other_key, actuals_key])
        rebuild = REBUILD_ON_UPDATE

        @dg.asset(
            key=self.model_key, deps=[actuals_key, MANIFEST_KEY], automation_condition=rebuild
        )
        def model(): ...

        @dg.asset(key=self.spine_key, deps=[MANIFEST_KEY], automation_condition=rebuild)
        def spine(): ...

        self.ods, self.models = [other, actuals], [model, spine]
        self.defs = dg.Definitions(assets=[other, actuals, manifest, model, spine])
        self.instance = dg.DagsterInstance.ephemeral()
        self.cursor = None

    def requested(self):
        result = dg.evaluate_automation_conditions(self.defs, self.instance, cursor=self.cursor)
        self.cursor = result.cursor
        keys = (self.model_key, self.spine_key)
        return {key for key in keys if result.get_num_requested(key)}

    def parse(self, day):
        dg.materialize(self.ods, partition_key=day, instance=self.instance)

    def build(self):
        dg.materialize(self.models, instance=self.instance)
        assert self.requested() == set()


def test_models_rebuild_after_any_parse_even_with_a_day_never_parsed():
    s = StandIns()
    both = {s.model_key, s.spine_key}
    assert s.requested() == set()
    s.parse("2026-01-02")
    assert s.requested() == both
    s.build()
    s.parse("2026-01-04")  # 2026-01-03 never parsed, as after a failed capture: no block
    assert s.requested() == both
    s.build()
    s.parse("2026-01-02")  # an old day parsed again
    assert s.requested() == both


def test_models_wait_for_the_whole_parse_not_its_first_step():
    # On 2026-11-17 the spine rebuilt after the first step reported the manifest, ahead of the DWD
    # it is joined with, and D-2 came out incomplete until the next rebuild.
    s = StandIns()
    seen = []
    s.during_actuals = lambda: seen.append(s.requested())
    assert s.requested() == set()

    s.parse("2026-01-02")

    assert seen == [set()]  # the manifest was updated, but the parse was still running
    assert s.requested() == {s.model_key, s.spine_key}  # then both, together

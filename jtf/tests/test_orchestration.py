import json
from collections import Counter
from datetime import UTC, datetime

import dagster as dg

from conftest import FIXTURES
from jtf.ingest.landed import SIDECAR
from jtf.ingest.sources import SOURCES
from jtf.orchestration.dbt import DBT_PROJECT, LEVELS, REBUILD_ON_UPDATE, dbt_models, group_of
from jtf.orchestration.definitions import defs
from jtf.orchestration.ml import (
    BACKTEST,
    BESIDE_TEPCO,
    BESIDE_TEPCO_METRICS,
    OUTCOMES,
    PREDICTIONS,
    REPLAY,
    solar_backtest_metrics,
    solar_predictions,
)
from jtf.orchestration.ods import (
    CATCH_UP,
    FILE_CHECKS,
    MANIFEST_KEY,
    MANIFEST_SPEC,
    ODS_ASSETS,
    PARSE_WHEN_LANDED,
    ROWS_AS_LOADED,
    ods_key,
)
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
    checks = {c.check_name: c.passed for c in result.get_asset_check_evaluations()}
    assert checks == {FILE_CHECKS: True, ROWS_AS_LOADED: True}
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


def test_dbt_reads_only_assets_made_here_so_lineage_runs_from_raw_files():
    # ODS and its manifest, parsed from raw files, and the predictions a model run writes.
    produced = set(dbt_models.keys)
    read = {dep.asset_key for spec in dbt_models.specs for dep in spec.deps} - produced
    assert read <= {a.key for a in ODS_ASSETS} | {MANIFEST_KEY, PREDICTIONS}


def test_the_backtest_runs_from_the_features_through_dbt_to_its_metrics_by_hand():
    specs = {spec.key: spec for spec in dbt_models.specs}
    predictions = solar_predictions.specs_by_key[PREDICTIONS]
    assert dg.AssetKey(["dws", "dws_solar_features"]) in {d.asset_key for d in predictions.deps}
    assert PREDICTIONS in {d.asset_key for d in specs[OUTCOMES].deps}
    metrics = next(iter(solar_backtest_metrics.specs))
    assert {d.asset_key for d in metrics.deps} == {OUTCOMES}
    # A backtest is a deliberate run: neither Python asset starts by itself.
    assert predictions.automation_condition is None
    assert metrics.automation_condition is None


def test_every_dbt_test_is_a_check_of_its_own():
    """Dagster keys a check by its asset and the test's name, and keeps one per key. dbt names a
    generic test from its arguments with the operators dropped, so `a = b` and `a <= b` on one model
    would share a check and one result would hide the other: such a test is named in its YAML."""
    manifest = json.loads(DBT_PROJECT.manifest_path.read_text())
    checked = Counter()
    for node in manifest["nodes"].values():
        if node["resource_type"] != "test":
            continue
        ref = (node["config"].get("meta") or {}).get("dagster", {}).get("ref", {}).get("name")
        reads = [uid for uid in node["depends_on"]["nodes"] if uid.startswith(("model.", "seed."))]
        attached = node.get("attached_node") or (reads[0] if len(reads) == 1 else None)
        model = ref or (attached.split(".")[-1] if attached else None)
        if model:
            checked[(model, node["name"])] += 1
    assert [key for key, count in checked.items() if count > 1] == []
    assert len(dbt_models.check_keys) == checked.total()


def test_models_rebuild_on_their_own_and_seeds_only_by_hand():
    nodes = json.loads(DBT_PROJECT.manifest_path.read_text())["nodes"].values()
    seeds = {n["name"] for n in nodes if n["resource_type"] == "seed"}
    for spec in dbt_models.specs:
        assert spec.key.path[0] in {"dim", "dwd", "dws", "ads"}, spec.key
        expected = None if spec.key.path[-1] in seeds else REBUILD_ON_UPDATE
        assert spec.automation_condition == expected, spec.key


def test_assets_are_grouped_by_layer_and_in_ads_by_application():
    graph = defs.resolve_asset_graph()
    groups = {key: graph.get(key).group_name for key in graph.get_all_asset_keys()}
    expected = {key: group_of(key.path[0], key.path[-1]) for key in groups}
    assert groups == expected
    assert {g for g in groups.values() if g.startswith("ads")} == {"ads_ml"}


def test_the_manifest_is_downstream_of_every_ods_asset():
    assert {dep.asset_key for dep in MANIFEST_SPEC.deps} == {a.key for a in ODS_ASSETS}


class StandIns:
    """Three ODS assets, the manifest and four models shaped like the real ones, with the real
    automation condition: `captures` reads the manifest (dwd_source_captures), `model` an ODS
    table and the captures (dwd_tepco_actuals), `spine` the captures (dws_decision_slots), and
    `reader` the spine and the model (dws_solar_features).

    Like the real ones, the ODS stand-ins report the manifest from their own step, and one run
    parses all three, in order: `other`, `actuals` (which the model reads), `late`.
    `during_actuals` and `during_late` run inside those steps, while the run is still going.
    """

    def __init__(self):
        days = dg.DailyPartitionsDefinition(start_date="2026-01-01", timezone="Asia/Tokyo")
        other_key, actuals_key = dg.AssetKey(["ods", "other"]), dg.AssetKey(["ods", "actuals"])
        late_key = dg.AssetKey(["ods", "late"])
        self.captures_key = dg.AssetKey(["dwd", "captures"])
        self.model_key = dg.AssetKey(["dwd", "model"])
        self.spine_key = dg.AssetKey(["dws", "spine"])
        self.reader_key = dg.AssetKey(["dws", "reader"])
        self.all = {self.captures_key, self.model_key, self.spine_key, self.reader_key}
        self.during_actuals = self.during_late = lambda: None

        @dg.asset(key=other_key, partitions_def=days)
        def other(context: dg.AssetExecutionContext):
            context.log_event(dg.AssetMaterialization(asset_key=MANIFEST_KEY))

        @dg.asset(key=actuals_key, deps=[other_key], partitions_def=days)
        def actuals(context: dg.AssetExecutionContext):
            self.during_actuals()
            context.log_event(dg.AssetMaterialization(asset_key=MANIFEST_KEY))

        @dg.asset(key=late_key, deps=[actuals_key], partitions_def=days)
        def late(context: dg.AssetExecutionContext):
            self.during_late()
            context.log_event(dg.AssetMaterialization(asset_key=MANIFEST_KEY))

        manifest = dg.AssetSpec(MANIFEST_KEY, deps=[other_key, actuals_key, late_key])
        rebuild = REBUILD_ON_UPDATE

        @dg.asset(key=self.captures_key, deps=[MANIFEST_KEY], automation_condition=rebuild)
        def captures(): ...

        @dg.asset(
            key=self.model_key, deps=[actuals_key, self.captures_key], automation_condition=rebuild
        )
        def model(): ...

        @dg.asset(key=self.spine_key, deps=[self.captures_key], automation_condition=rebuild)
        def spine(): ...

        @dg.asset(
            key=self.reader_key, deps=[self.spine_key, self.model_key], automation_condition=rebuild
        )
        def reader(): ...

        self.ods, self.models = [other, actuals, late], [captures, model, spine, reader]
        self.defs = dg.Definitions(assets=[*self.ods, manifest, *self.models])
        self.instance = dg.DagsterInstance.ephemeral()
        self.cursor = None

    def requested(self):
        result = dg.evaluate_automation_conditions(self.defs, self.instance, cursor=self.cursor)
        self.cursor = result.cursor
        return {key for key in self.all if result.get_num_requested(key)}

    def parse(self, day):
        dg.materialize(self.ods, partition_key=day, instance=self.instance)

    def build(self):
        dg.materialize(self.models, instance=self.instance)
        assert self.requested() == set()


def test_models_rebuild_after_any_parse_even_with_a_day_never_parsed():
    s = StandIns()
    assert s.requested() == set()
    s.parse("2026-01-02")
    assert s.requested() == s.all
    s.build()
    s.parse("2026-01-04")  # 2026-01-03 never parsed, as after a failed capture: no block
    assert s.requested() == s.all
    s.build()
    s.parse("2026-01-02")  # an old day parsed again
    assert s.requested() == s.all


def test_models_wait_for_the_whole_parse_not_its_first_step():
    # On 2026-11-17 the spine rebuilt after the first step reported the manifest, ahead of the DWD
    # it is joined with, and D-2 came out incomplete until the next rebuild.
    s = StandIns()
    seen = []
    s.during_actuals = lambda: seen.append(s.requested())
    assert s.requested() == set()

    s.parse("2026-01-02")

    assert seen == [set()]  # the manifest was updated, but the parse was still running
    assert s.requested() == s.all  # then all of them, together


def test_no_model_builds_while_anything_upstream_of_it_is_being_parsed():
    # On 2026-11-19 a tick fell in the last second of the parse run. The model's own ODS table
    # was done, so it and its reader were requested, while the spine, further from the step
    # still running than the guard looked, waited a tick: the reader was built on the old spine.
    s = StandIns()
    seen = []
    s.during_late = lambda: seen.append(s.requested())
    assert s.requested() == set()

    s.parse("2026-01-02")

    assert seen == [set()]  # the model's table was parsed, but another step was still running
    assert s.requested() == s.all  # then all of them, in one run


def steps_to_a_parse(graph, key, seen: dict) -> int | None:
    """The longest way from an asset up to an ODS asset, which a parse builds, in steps along
    the graph; None if no ODS asset is upstream of it."""
    if key not in seen:
        parents = graph.get(key).parent_keys
        parsed = {a.key for a in ODS_ASSETS}
        ways = [1 for parent in parents if parent in parsed] + [
            1 + further
            for parent in parents
            if (further := steps_to_a_parse(graph, parent, seen)) is not None
        ]
        seen[key] = max(ways, default=None)
    return seen[key]


def test_the_guard_looks_as_far_upstream_as_any_model_is_from_a_parse():
    graph, seen = defs.resolve_asset_graph(), {}
    steps = {
        spec.key: steps_to_a_parse(graph, spec.key, seen) or 0
        for spec in dbt_models.specs
        if spec.automation_condition
    }
    furthest = max(steps, key=steps.get)
    assert steps[dg.AssetKey(["dwd", "dwd_source_captures"])] == 2  # through the manifest
    assert steps[furthest] <= LEVELS, (furthest, steps[furthest])


def test_a_replay_and_a_backtest_are_jobs_with_their_assets():
    graph = defs.resolve_asset_graph()
    assert {defs.resolve_job_def(n).name for n in ("replay", "backtest")} == {"replay", "backtest"}

    # Every run is also set beside TEPCO's forecast, where their days overlap
    beside_tepco = {BESIDE_TEPCO, BESIDE_TEPCO_METRICS}
    assert REPLAY.selection.resolve(graph) == {PREDICTIONS, OUTCOMES} | beside_tepco
    assert (
        BACKTEST.selection.resolve(graph)
        == {
            PREDICTIONS,
            OUTCOMES,
            dg.AssetKey(["ml", "solar_backtest_metrics"]),
        }
        | beside_tepco
    )
    assert graph.get(BESIDE_TEPCO_METRICS).parent_keys == {BESIDE_TEPCO}
    assert PREDICTIONS in graph.get(BESIDE_TEPCO).parent_keys
    assert REPLAY.config["ops"][solar_predictions.op.name]["config"]["mode"] == "what-if"


def test_every_parse_asset_catches_up_on_the_days_missed_while_dagster_was_off():
    conditions = {
        asset.key: asset.automation_conditions_by_key.get(asset.key) for asset in ODS_ASSETS
    }
    assert conditions and set(conditions.values()) == {PARSE_WHEN_LANDED}


def test_days_whose_files_landed_while_dagster_was_off_are_all_parsed_once_it_is_back():
    days = dg.DailyPartitionsDefinition(start_date="2026-01-01", timezone="Asia/Tokyo")
    raw = dg.AssetSpec(dg.AssetKey(["raw", "source"]), partitions_def=days)

    @dg.asset(
        key=dg.AssetKey(["ods", "table"]),
        deps=[raw.key],
        partitions_def=days,
        automation_condition=PARSE_WHEN_LANDED,
    )
    def table(): ...

    defs = dg.Definitions(assets=[raw, table])
    instance = dg.DagsterInstance.ephemeral()
    now = datetime(2026, 3, 20, 10, 0, tzinfo=UTC)
    first = dg.evaluate_automation_conditions(defs, instance, evaluation_time=now)
    assert first.total_requested == 0

    # Dagster comes back after four days: the sensor records every day's files in one tick. A
    # file that turned up for a day ten weeks old is recorded too, and is beyond the catch-up.
    landed = ["2026-03-16", "2026-03-17", "2026-03-18", "2026-03-19", "2026-01-05"]
    for day in landed:
        instance.report_runless_asset_event(dg.AssetMaterialization(raw.key, partition=day))
    back = dg.evaluate_automation_conditions(
        defs, instance, cursor=first.cursor, evaluation_time=now
    )

    asked = back.get_requested_partitions(table.key)
    assert asked == set(landed[:4])
    assert CATCH_UP.days >= 28  # a few times a week, with room for a month away

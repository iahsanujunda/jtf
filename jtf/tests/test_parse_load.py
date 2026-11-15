from datetime import UTC, datetime

import pandas as pd
import pytest

from conftest import FIXTURES
from jtf.parse.base import conform
from jtf.parse.load import MANIFEST, parse_day
from jtf.parse.tepco_actuals import PARSER
from jtf.storage import LocalStore

NOW = datetime(2026, 11, 15, 9, 0, tzinfo=UTC)


def run(store, warehouse, batch="b1"):
    return parse_day(PARSER, store, warehouse, "2026-11-15", batch_id=batch, now=NOW)


def test_a_day_loads_each_file_once_and_leaves_other_layouts_alone(warehouse):
    store = LocalStore(FIXTURES)
    first = run(store, warehouse)

    assert first.ok and len(first.loaded) == 2 and first.rows == 202
    manifest = warehouse.rows(MANIFEST)
    assert sorted(manifest["status"]) == ["parsed", "parsed"]
    assert not manifest["filename"].str.startswith("area-").any()  # the yearly layout's parser
    assert set(warehouse.rows("tepco_actuals")["batch_id"]) == {"b1"}

    again = run(store, warehouse, batch="b2")
    assert again.skipped == 2 and not again.loaded
    assert len(warehouse.rows("tepco_actuals")) == 202
    assert len(warehouse.rows(MANIFEST)) == 2


def test_a_load_stopped_before_its_manifest_is_finished_without_duplicating_rows(warehouse):
    store = LocalStore(FIXTURES)
    run(store, warehouse)
    warehouse.tables[MANIFEST].clear()  # as if the manifest write never happened

    again = run(store, warehouse, batch="b2")

    assert not again.loaded
    assert len(warehouse.rows("tepco_actuals")) == 202
    assert len(warehouse.rows(MANIFEST)) == 2


def test_a_refused_file_is_recorded_and_tried_again(tmp_path, warehouse):
    store = LocalStore(tmp_path)
    source = LocalStore(FIXTURES)
    for name in (
        "20261115T020416Z_eria_jukyu_202611_03.csv",
        "20261115T045300Z_eria_jukyu_202402_03.csv",
    ):
        path = f"tepco_actuals/2026-11-15/{name}"
        body = source.get(path)
        if "202611" in name:
            body = body.replace("単位[MW平均]".encode("cp932"), "単位[万kW]".encode("cp932"))
        store.put_new(path, body, "text/csv")
        store.put_new(path + ".meta.json", source.get(path + ".meta.json"), "application/json")

    first = run(store, warehouse)
    assert len(first.failed) == 1 and len(first.loaded) == 1
    failed = warehouse.rows(MANIFEST).query("status == 'failed'")
    assert "unit row" in failed["note"].iloc[0]

    again = run(store, warehouse, batch="b2")
    assert len(again.failed) == 1 and again.skipped == 1


def test_rows_must_match_the_table_exactly():
    with pytest.raises(ValueError, match="extra"):
        conform(pd.DataFrame({"path": ["x"], "surprise": [1]}), PARSER.schema)

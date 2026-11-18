"""ODS fingerprints: recorded at load, checked after every load, reread only where the free
partition metadata says something may have changed (notebooks/07_ods_fingerprints.ipynb)."""

from datetime import UTC, date, datetime

from conftest import FIXTURES
from jtf.parse.fingerprint import FINGERPRINTS, backfill, verify
from jtf.parse.load import MANIFEST, parse_day
from jtf.parse.tepco_actuals import PARSER
from jtf.storage import LocalStore

NOW = datetime(2026, 11, 15, 9, 0, tzinfo=UTC)
DAY = date(2026, 11, 15)
TABLE = PARSER.table


def load(warehouse, batch="b1"):
    return parse_day(PARSER, LocalStore(FIXTURES), warehouse, "2026-11-15", batch_id=batch, now=NOW)


def one_file(warehouse) -> str:
    return sorted(set(warehouse.rows(TABLE)["source_file"]))[0]


def test_a_load_records_one_fingerprint_per_file_it_loaded_and_none_when_it_skips(warehouse):
    first = load(warehouse)

    recorded = warehouse.rows(FINGERPRINTS)
    assert first.fingerprinted == 2
    assert sorted(recorded["source_file"]) == sorted(first.loaded)
    assert set(recorded["basis"]) == {"load"} and set(recorded["ods_table"]) == {TABLE}
    assert recorded["row_count"].sum() == first.rows

    again = load(warehouse, batch="b2")
    assert again.fingerprinted == 0 and len(warehouse.rows(FINGERPRINTS)) == 2


def test_a_load_stopped_after_its_rows_gets_its_fingerprints_once(warehouse):
    load(warehouse)
    warehouse.tables[MANIFEST].clear()
    warehouse.tables[FINGERPRINTS].clear()  # as if the load stopped right after the rows

    again = load(warehouse, batch="b2")
    assert again.fingerprinted == 2 and not again.loaded

    warehouse.tables[MANIFEST].clear()  # stopped after the fingerprints this time
    third = load(warehouse, batch="b3")
    assert third.fingerprinted == 0 and len(warehouse.rows(FINGERPRINTS)) == 2
    assert verify(warehouse, TABLE).ok


def test_nothing_is_reread_while_nothing_changed(warehouse):
    load(warehouse)
    warehouse.fingerprinted_days.clear()

    verification = verify(warehouse, TABLE)

    assert verification.ok and verification.reread == []
    assert warehouse.fingerprinted_days == []


def test_a_value_changed_in_place_is_found_by_name(warehouse):
    load(warehouse)
    path = one_file(warehouse)

    def update(rows):
        rows.loc[rows.index[rows["source_file"] == path][0], "solar"] += 1
        return rows

    warehouse.change(TABLE, update)
    verification = verify(warehouse, TABLE)

    assert verification.reread == [DAY]
    assert list(verification.problems) == [f"{path} (1)"]
    assert verification.problems[f"{path} (1)"].startswith("rows changed since recorded")


def test_a_deleted_row_is_found_even_when_the_partition_time_does_not_move(warehouse):
    load(warehouse)
    path = one_file(warehouse)

    def delete(rows):
        return rows.drop(rows.index[rows["source_file"] == path][0])

    warehouse.change(TABLE, delete, moves=False)
    verification = verify(warehouse, TABLE)

    assert verification.reread == [DAY]
    assert list(verification.problems) == [f"{path} (1)"]


def test_a_file_loaded_twice_is_found(warehouse):
    load(warehouse)
    path = one_file(warehouse)
    rows = warehouse.rows(TABLE)
    warehouse.append(TABLE, PARSER.schema, "source_file", rows[rows["source_file"] == path])

    verification = verify(warehouse, TABLE)

    assert list(verification.problems) == [f"{path} (1)"]


def test_only_the_partitions_that_moved_are_reread(warehouse):
    load(warehouse)
    rows = warehouse.rows(TABLE)
    other = rows.assign(extraction_date=date(2026, 11, 16), source_file=rows["source_file"] + "x")
    warehouse.append(TABLE, PARSER.schema, "source_file", other)
    backfill(warehouse, TABLE, batch_id="bf")
    warehouse.fingerprinted_days.clear()

    def update(rows):
        rows.loc[rows.index[rows["extraction_date"] == DAY][0], "solar"] += 1
        return rows

    warehouse.change(TABLE, update)  # touches the 15th only
    verification = verify(warehouse, TABLE)

    assert warehouse.fingerprinted_days == [[DAY]]
    assert len(verification.problems) == 1


def test_a_file_never_fingerprinted_is_reported_until_backfill_records_it(warehouse):
    load(warehouse)
    warehouse.tables[FINGERPRINTS].clear()  # loaded before fingerprints existed

    before = verify(warehouse, TABLE)
    assert set(before.problems.values()) == {"no fingerprint recorded"}

    assert backfill(warehouse, TABLE, batch_id="bf") == 2
    assert set(warehouse.rows(FINGERPRINTS)["basis"]) == {"backfill"}
    assert verify(warehouse, TABLE).ok
    assert backfill(warehouse, TABLE, batch_id="bf2") == 0


def test_a_full_recheck_reads_every_partition_and_finds_what_the_screen_cannot(warehouse):
    load(warehouse)
    path = one_file(warehouse)

    def update(rows):
        rows.loc[rows.index[rows["source_file"] == path][0], "solar"] += 1
        return rows

    warehouse.change(TABLE, update, moves=False)  # a change no metadata shows
    assert verify(warehouse, TABLE).ok
    assert list(verify(warehouse, TABLE, everything=True).problems) == [f"{path} (1)"]


def test_a_file_whose_fingerprint_was_recorded_twice_is_found(warehouse):
    load(warehouse)
    recorded = warehouse.rows(FINGERPRINTS)
    warehouse.tables[FINGERPRINTS].append(recorded.iloc[:1])  # two parses racing, say

    verification = verify(warehouse, TABLE)

    assert set(verification.problems.values()) == {"recorded 2 times"}


def test_a_file_whose_rows_are_gone_is_found(warehouse):
    load(warehouse)
    path = one_file(warehouse)

    warehouse.change(TABLE, lambda rows: rows[rows["source_file"] != path])
    verification = verify(warehouse, TABLE)

    assert verification.problems == {f"{path} (1)": "its rows are gone"}

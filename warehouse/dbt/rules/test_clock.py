"""No model reads the clock (rules/README.md, the third rule): T is always passed in, so a replay
gives the same answer whenever it runs.

Two checks, because a clock read in Jinja compiles to a literal that no check over SQL can tell
from any other timestamp: sqlglot over every model's compiled SQL, and a pattern over the raw Jinja
of every model and project macro. Data tests may read the clock: they check, they are not read.
"""

import re

import pytest
import sqlglot
from sqlglot import exp

SQL_CLOCK = (exp.CurrentDate, exp.CurrentDatetime, exp.CurrentTime, exp.CurrentTimestamp)
JINJA_CLOCK = re.compile(r"run_started_at|modules\.datetime|modules\.time\b")


def sql_clock_reads(sql: str) -> list[str]:
    tree = sqlglot.parse_one(sql, dialect="bigquery")
    found = [n.sql(dialect="bigquery") for n in tree.find_all(*SQL_CLOCK)]
    return found + [n.sql() for n in tree.find_all(exp.Anonymous) if n.name.upper() == "NOW"]


@pytest.mark.parametrize(
    "sql, reads",
    [
        ("select current_date()", True),
        ("select current_date('Asia/Tokyo')", True),
        ("select current_datetime('Asia/Tokyo')", True),
        ("select current_time()", True),
        ("select current_timestamp()", True),
        ("select now()", True),
        ("select timestamp_sub(current_timestamp(), interval 1 day)", True),
        ("select timestamp '2026-11-18 09:00:00+09'", False),
        ("select date_add(date(max(known_at), 'Asia/Tokyo'), interval 1 day) from t", False),
    ],
)
def test_the_sql_check_finds_every_clock_read(sql, reads):
    assert bool(sql_clock_reads(sql)) is reads


@pytest.mark.parametrize(
    "jinja, reads",
    [
        ("select '{{ run_started_at }}' as built_at", True),
        ("{% set today = modules.datetime.date.today() %}", True),
        ("{{ modules.time.time() }}", True),
        ("time({{ var('decision_hour_jst') }}, 0, 0)", False),
        ("{{ ref('dws_decision_slots') }}", False),
    ],
)
def test_the_jinja_check_finds_every_clock_read(jinja, reads):
    assert bool(JINJA_CLOCK.search(jinja)) is reads


def test_no_model_reads_the_clock_in_its_sql(compiled):
    found = {
        node["name"]: hits
        for node in compiled["nodes"].values()
        if node["resource_type"] == "model" and (hits := sql_clock_reads(node["compiled_code"]))
    }
    assert not found, f"pass T in instead: {found}"


def test_no_model_or_macro_reads_the_clock_in_jinja(manifest):
    code = {
        node["name"]: node["raw_code"]
        for node in manifest["nodes"].values()
        if node["resource_type"] == "model"
    } | {
        m["name"]: m["macro_sql"] for m in manifest["macros"].values() if m["package_name"] == "jtf"
    }
    found = sorted(name for name, text in code.items() if JINJA_CLOCK.search(text))
    assert not found, f"pass T in instead: {found}"

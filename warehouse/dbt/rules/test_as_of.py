"""Every read of a versioned DWD model in DWS goes through `as_of` (README.md, the second rule).

Checked with sqlglot over the SQL dbt compiles, scope by scope. Each table a DWS query reads that
is a versioned DWD model (one documenting known_at and known_until) must carry, on that very alias
and in the same scope (its join's ON, or the scope's WHERE), the predicate `as_of` writes:

    v.known_at <= T and (v.known_until is null or T < v.known_until)

with one T, a column named decision_time. Strict by choice (agreed 2026-11-18): the same predicate
written another way, or a read through a CTE with the predicate outside it, fails too. Write it
with the macro, on the table itself. Outside the rule: DIM, DWS reading DWS, labels in ADS.
"""

from typing import NamedTuple

import pytest
import sqlglot
from sqlglot import exp
from sqlglot.optimizer.scope import traverse_scope

# Reads exempt by name, each with its reason in rules/README.md; the last test fails once one is no
# longer there. The spine reads how far the captures reach now: it decides which decision times
# exist and feeds no value.
EXEMPT = {("dws_decision_slots", "dwd_source_captures")}


class Read(NamedTuple):
    table: str
    alias: str
    t: str | None  # the decision time of its as_of predicate, None without one


def conjuncts(condition: exp.Expression | None) -> list[exp.Expression]:
    if condition is None:
        return []
    condition = condition.unnest()
    if isinstance(condition, exp.And):
        return conjuncts(condition.left) + conjuncts(condition.right)
    return [condition]


def is_column(e: exp.Expression, name: str, alias: str | None = None) -> bool:
    e = e.unnest()
    return isinstance(e, exp.Column) and e.name == name and (alias is None or e.table == alias)


def as_of_t(conditions: list[exp.Expression], alias: str) -> str | None:
    """The T of an as_of predicate on `alias` among the conditions, both halves on the same T."""
    known = {
        c.expression.sql()
        for c in conditions
        if isinstance(c, exp.LTE)
        and is_column(c.this, "known_at", alias)
        and is_column(c.expression, "decision_time")
    }
    for c in conditions:
        if not isinstance(c, exp.Or):
            continue
        left, right = c.left.unnest(), c.right.unnest()
        if (
            isinstance(left, exp.Is)
            and is_column(left.this, "known_until", alias)
            and isinstance(left.expression, exp.Null)
            and isinstance(right, exp.LT)
            and is_column(right.expression, "known_until", alias)
            and right.this.sql() in known
        ):
            return right.this.sql()
    return None


def reads(sql: str, versioned: set[str]) -> list[Read]:
    """Every read of a versioned table in the query, with the T it is read at."""
    found = []
    for scope in traverse_scope(sqlglot.parse_one(sql, dialect="bigquery")):
        where = scope.expression.args.get("where")
        for alias, (node, source) in scope.selected_sources.items():
            if not isinstance(source, exp.Table) or source.name not in versioned:
                continue
            join = node if isinstance(node, exp.Join) else node.find_ancestor(exp.Join)
            on = join.args.get("on") if isinstance(join, exp.Join) and join.this is source else None
            conditions = conjuncts(on) + conjuncts(where.this if where else None)
            found.append(Read(source.name, alias, as_of_t(conditions, alias)))
    return found


SPINE = "`p`.`dev_dws`.`dws_decision_slots`"
DWD = "`p`.`dev_dwd`.`dwd_capacity_area`"
AS_OF = (
    "(v.known_at <= d.decision_time and (v.known_until is null or d.decision_time < v.known_until))"
)
JOINED = f"select d.decision_time, v.part from {SPINE} as d inner join {DWD} as v on {AS_OF}"

CASES = {
    # name: (sql, passes)
    "joined with as_of": (JOINED, True),
    "as_of in the scope's WHERE": (
        f"select d.decision_time, v.part from {SPINE} as d cross join {DWD} as v where {AS_OF}",
        True,
    ),
    "the predicate on the spine's alias": (JOINED.replace("v.known", "d.known"), False),
    "half: no known_until": (JOINED.replace(AS_OF, "v.known_at <= d.decision_time"), False),
    "< instead of <=": (JOINED.replace("v.known_at <=", "v.known_at <"), False),
    # The leak of fault scenario F2 (docs/scenarios): right for a label, a leak in a feature
    "the latest version, as a label is read": (
        JOINED.replace(AS_OF, "v.known_until is null"),
        False,
    ),
    "T is the clock": (JOINED.replace("d.decision_time", "current_timestamp()"), False),
    "T is a literal": (JOINED.replace("d.decision_time", "timestamp '2026-11-18'"), False),
    "two different Ts": (JOINED.replace("d.decision_time < v", "e.decision_time < v"), False),
    "as_of OR-ed away": (JOINED + " or true", False),
    "a 'latest' subquery, no T": (
        f"select d.decision_time, v.part from {SPINE} as d join (select * from {DWD} qualify "
        "row_number() over (partition by part order by known_at desc) = 1) as v on true",
        False,
    ),
    "a second, bare read": (f"{JOINED} union all select null, part from {DWD}", False),
    "through a CTE": (
        f"with v as (select * from {DWD}) "
        f"select d.decision_time, v.part from {SPINE} as d join v on {AS_OF}",
        False,
    ),
    # Correct, and refused by choice: one way to write it
    "the comparison's sides swapped": (
        JOINED.replace("v.known_at <= d.decision_time", "d.decision_time >= v.known_at"),
        False,
    ),
    "the versions in FROM, the spine joined": (
        f"select d.decision_time, v.part from {DWD} as v inner join {SPINE} as d on {AS_OF}",
        False,
    ),
}


@pytest.mark.parametrize("sql, passes", CASES.values(), ids=CASES.keys())
def test_the_rule_tells_the_cases_apart(sql, passes):
    found = reads(sql, {"dwd_capacity_area"})
    assert found
    assert all(r.t for r in found) is passes


def versioned(manifest: dict) -> set[str]:
    """The DWD models others read through as_of: those documenting known_at and known_until."""
    return {
        node["alias"]
        for node in manifest["nodes"].values()
        if node["resource_type"] == "model"
        and node["config"]["schema"] == "dwd"
        and {"known_at", "known_until"} <= set(node["columns"])
    }


def dws_reads(compiled: dict) -> dict[str, list[Read]]:
    tables = versioned(compiled)
    return {
        node["name"]: reads(node["compiled_code"], tables)
        for node in compiled["nodes"].values()
        if node["resource_type"] == "model" and node["config"]["schema"] == "dws"
    }


def test_every_read_of_a_versioned_dwd_model_in_dws_is_through_as_of(compiled):
    bare = [
        f"{model} reads {r.table} as {r.alias} without as_of on that alias against a decision_time"
        for model, found in dws_reads(compiled).items()
        for r in found
        if r.t is None and (model, r.table) not in EXEMPT
    ]
    assert not bare, (
        "join it with {{ as_of('<alias>', '<spine>.decision_time') }} on the table itself:\n"
        + "\n".join(bare)
    )


def test_each_exemption_is_still_needed(compiled):
    used = {
        (model, r.table) for model, found in dws_reads(compiled).items() for r in found if not r.t
    }
    assert not (EXEMPT - used), f"no longer read without as_of, drop: {EXEMPT - used}"

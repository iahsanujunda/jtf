# dbt tests

dbt's own folder for singular data tests: SQL queries that must return no rows, such as "no solar output at night after conversion" (F4) or "no slot above installed capacity" (F5). Generic tests (unique, not null, accepted ranges) live next to the models in their YAML; dbt unit tests for the `as_of` macro and unit conversions live there too.

A test is named `<model>__<what it checks>`. One that reads more than one model names its model in `meta.dagster.ref` as well, so Dagster shows it as a check on that asset.

[generic/](generic/) holds the tests applied in YAML with arguments: `expression_is_true`, `unique_combination`, and the two that read the ODS manifest for `known_at`, `known_at_within_fetches` (no `known_at` after the newest fetch of the model's sources) and `rule_before_capture_start` (no rule-dated version at or after the source's first daily fetch). A generic test whose default name could match another's on the same model is named in its YAML (`name: <model>__<what it checks>`): dbt builds the default from the arguments with the operators dropped, so `a = b` and `a <= b` would share a name, and Dagster keeps one check per name (`jtf/tests/test_orchestration.py` fails if two do).

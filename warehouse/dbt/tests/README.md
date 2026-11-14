# dbt tests

dbt's own folder for singular data tests: SQL queries that must return no rows, such as "no solar output at night after conversion" (F4) or "no slot above installed capacity" (F5). Generic tests (unique, not null, accepted ranges) live next to the models in their YAML; dbt unit tests for the `as_of` macro and unit conversions live there too.

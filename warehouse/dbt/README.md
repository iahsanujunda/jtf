# dbt

dbt-bigquery project, one dataset per layer. Dagster loads the models as assets through `dagster-dbt`, so lineage runs from raw files through dbt to predictions.

| Layer | Holds | Phase |
| --- | --- | --- |
| [ODS](models/ods/) | Parsed rows from raw files, append-only | 1 |
| [DWD](models/dwd/) | Typed detail in MW and JST slot start, every version kept | 1 |
| [DWS](models/dws/) | Features per decision time and slot, for every decision time | 1 |
| [ADS](models/ads/) | Published forecasts, input snapshots, replay outputs | 1 (replays), 2 (daily) |
| [monitoring](models/monitoring/) | Drift, accuracy, dashboard snapshots | 3 |

Targets: `dev` (your own dataset prefix in the dev project), `ci` (a throwaway dataset per PR), `prod` (written only by CI).

Rules: DWS reads inputs only through the as-of macro in [macros/](macros/); no `CURRENT_DATE()` or `NOW()` anywhere. CI enforces both with SQL rules in [rules/](rules/).

## Tests

The warehouse owns every test of its SQL:

- **dbt data tests** on models (uniqueness, not-null, accepted ranges, 48 slots per day) and singular tests in [tests/](tests/)
- **dbt unit tests** (YAML `unit_tests:`) for the logic that must never be wrong: the `as_of` macro, unit conversions, slot normalisation
- **SQL rules** in [rules/](rules/): sqlglot checks over compiled SQL

All run in a throwaway PR dataset in the dev project.

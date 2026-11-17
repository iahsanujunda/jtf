# dbt

dbt-bigquery project, one dataset per layer. Dagster loads the models as assets through `dagster-dbt`, so lineage runs from raw files through dbt to predictions.

| Layer | Holds | Phase |
| --- | --- | --- |
| [ODS](models/ods/) | Parsed rows from raw files, append-only | 1 |
| [DIM](models/dim/) | What nothing is learned about over time: the sun per point and slot, daylight, and the seeds | 1 |
| [DWD](models/dwd/) | Typed detail in MW and JST slot start, every version kept | 1 |
| [DWS](models/dws/) | Features per decision time and slot, for every decision time | 1 |
| [ADS](models/ads/) | Outputs: labels, predictions and their outcomes; published forecasts' input snapshots | 1 (labels, backtests), 2 (daily) |
| [monitoring](models/monitoring/) | Drift, accuracy, dashboard snapshots | 3 |

Targets: `dev` (datasets named `<prefix>_<layer>` in the dev project, the prefix from `JTF_DBT_DATASET_PREFIX`), `ci` (a throwaway prefix per PR), `prod` (written only by CI). Settings come from the environment, through [profiles.yml](profiles.yml); locally, `make dbt` reads them from `.env` and runs `dbt build`, and `make dbt ARGS="test --select dwd_tepco_actuals"` runs anything else.

Rules: no layer reads one above it, and only DWD reads ODS; DWS reads versioned DWD models only through the as-of macro in [macros/](macros/), and DIM directly; no `CURRENT_DATE()` or `NOW()` anywhere. The checks in [rules/](rules/) enforce them, as they are built: the layers' reads now, with pytest.

## Tests

The warehouse owns every test of its SQL:

- **dbt data tests** on models (uniqueness, not-null, accepted ranges, 48 slots per day) and singular tests in [tests/](tests/)
- **dbt unit tests** (YAML `unit_tests:`) for the logic that must never be wrong: the `as_of` macro, unit conversions, slot normalisation
- **Rules** in [rules/](rules/): manifest and sqlglot checks over compiled SQL, and a fingerprint test that ODS rows never change

In CI the unit tests, an empty build and the rules run in a throwaway PR dataset that reads no real data. The data tests run on real data after every build, as Dagster checks; the fingerprint after every load, once built (before step 6).

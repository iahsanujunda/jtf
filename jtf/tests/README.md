# jtf tests

Unit tests for the `jtf` package only. They run offline with `file://` storage and need no GCP credentials.

| Area | Proves |
| --- | --- |
| Parsers | Each real source file in `fixtures/` (Shift_JIS kept as is) becomes the right ODS rows: slots, units as published, `known_at` taken from the raw file's sidecar |
| Storage | Raw writes are create-only: a second write to the same path fails |
| Fetch | Requests and sidecars are built correctly; no network in tests |
| Backtest | Folds, the persistence baseline and the metrics against hand-computed values: daylight slots with a label only, shares of capacity, every slot weighed alike |
| Orchestration | Dagster definitions load; partitions, sensors and checks are wired to the right assets |

Not here: SQL (owned by [warehouse/dbt](../../warehouse/dbt/)) and guarantees that need several components together (owned by [e2e_tests](../../e2e_tests/)).

Run: `uv run --package jtf pytest jtf/tests`

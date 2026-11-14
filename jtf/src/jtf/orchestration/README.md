# orchestration

Dagster definitions. Assets are thin wrappers over `jtf` functions: no logic lives here that can't be tested without Dagster.

| Piece | Phase |
| --- | --- |
| External `raw` assets, one per source, and the raw-bucket sensor that records each landed file | 1 |
| Freshness checks per capture group (morning by 09:00, `jepx` by 11:30, evening by 22:30) | 1 |
| Partitions: extraction day for raw and ODS; decision date from 2024-02-01 for DWS and predictions | 1 |
| ODS assets with declarative automation on new raw records | 1 |
| dbt assets through `dagster-dbt`; dbt tests as asset checks; single-run backfill policy for DWS | 1 |
| Backtest, train and replay jobs with run config (`mode`, `T` range, `fault`) | 1 |
| Daily prediction schedule at 09:05 JST; evaluation sensor on new actuals | 2 |
| Retrain job, shadow runs, export job | 2–3 |
| Run-failure sensor that starts AI triage | 5 |

Every asset carries a code version, so a release that changes one marks its partitions out of date. Every run is tagged with its image digest.

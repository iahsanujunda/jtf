# orchestration

Dagster definitions. Assets are thin wrappers over `jtf` functions: no logic lives here that can't be tested without Dagster.

| Piece | Phase |
| --- | --- |
| External `raw` assets, one per source, and the raw-bucket sensor that records each landed file | 1 |
| Freshness checks per capture source (08:45 sources by 09:00, `jepx_spot` by 11:30, next-day sources by 22:30) | 1 |
| Partitions: extraction day for raw and ODS; decision date from 2024-02-16 for DWS and predictions (target day 2024-02-17, the first with full MSM radiation) | 1 |
| ODS assets with declarative automation on new raw records | 1 |
| dbt assets through `dagster-dbt`; dbt tests as asset checks; single-run backfill policy for DWS | 1 |
| Backtest, train and replay jobs with run config (`mode`, `T` range, `fault`) | 1 |
| Heartbeat schedule pinging the healthchecks.io check `jtf-dagster` every 15 minutes; run-failure sensor sending failed runs to its `/fail` | 1 |
| Daily prediction schedule at 09:05 JST; evaluation sensor on new actuals | 2 |
| Retrain job, shadow runs, export job | 2–3 |
| Run-failure sensor that starts AI triage | 5 |

Every asset carries a code version, so a release that changes one marks its partitions out of date. Every run is tagged with its image digest.

**Locally:** `make dagster` serves the UI on http://localhost:3000, keeps Dagster's state in `.dagster/` and reads settings from a gitignored `.env` at the repo root (names in [deploy/contract](../../../../deploy/contract/settings.example.env)): `JTF_RAW_URL` pointing at the prod raw bucket, read-only, and `JTF_BIGQUERY_PROJECT` at the dev project. Both sensors, `raw_bucket_sensor` and `automation`, start running. A day recorded before its ODS asset existed is loaded by hand (Materialize, pick the partition); from then on, a newly recorded capture starts the parse of its day on its own.

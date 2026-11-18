# cli

The `jtf` entry point. Each subcommand is a thin wrapper over a function in another subpackage, so logic is testable without the CLI.

| Command | Does | Phase |
| --- | --- | --- |
| `jtf fetch --source S [--initial \| --recapture] [--max-calls N]` | Download a source into the raw bucket: daily, the one-off history load, or the monthly check for rewrites | 1 |
| `jtf sources` | List the sources and the modes each supports | 1 |
| `jtf parse --source S --from D1 --to D2` | Raw files → ODS rows (normally run by Dagster's ODS assets) | 1 |
| `jtf ci create` / `jtf ci drop` | A CI run's throwaway datasets: an empty ODS from the parsers' schemas and an empty predictions table, so `dbt build --empty` reads no real data; only a `ci_<run id>` prefix is accepted | 1 |
| `jtf ods verify [--table T] [--all]` | Check that every file's rows in ODS still match the fingerprint recorded at their load, as the ODS assets do after each load; `--all` reads every file again | 1 |
| `jtf ods backfill-fingerprints [--table T]` | Fingerprint the files in ODS that have no record yet, as `backfill`; with Dagster quiet, as `jtf parse` | 1 |
| `jtf dagster update` | Bring the checkout the laptop's Dagster runs from up to `main` with no run in between: stop the sensors, wait for running runs, pull, reload, start the sensors (`make dagster-update`) | 1 |
| `jtf transform [--select …]` | Run dbt for the derived layers | 1 |
| `jtf backtest` / `jtf train` | Rolling backtest; train a model version | 1 |
| `jtf replay --from D1 --to D2 [--model M]` or `jtf replay --reproduce RUN` | The time machine: a what-if run over target days, or a reproduce of an earlier run, which must match it exactly; recorded in `ads_ml_runs` ([ml](../ml/README.md)) | 1 |
| `jtf predict --as-of T` | Forecast the 48 slots of D | 1 (laptop), 2 (scheduled) |
| `jtf daily --as-of T` | The whole daily path without Dagster: the fallback job | 2 |
| `jtf evaluate` | Accuracy, drift, dashboard snapshot | 3 |
| `jtf triage` | AI incident note for a failed step | 5 |

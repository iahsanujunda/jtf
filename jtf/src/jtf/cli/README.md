# cli

The `jtf` entry point. Each subcommand is a thin wrapper over a function in another subpackage, so logic is testable without the CLI.

| Command | Does | Phase |
| --- | --- | --- |
| `jtf fetch --source S [--initial]` | Download a source into the raw bucket | 1 |
| `jtf parse --source S --from D1 --to D2` | Raw files → ODS rows (normally run by Dagster's ODS assets) | 1 |
| `jtf transform [--select …]` | Run dbt for the derived layers | 1 |
| `jtf backtest` / `jtf train` | Rolling backtest; train a model version | 1 |
| `jtf replay --as-of T --mode reproduce\|what-if` | The time machine | 1 |
| `jtf predict --as-of T` | Forecast the 48 slots of D | 1 (laptop), 2 (scheduled) |
| `jtf daily --as-of T` | The whole daily path without Dagster: the fallback job | 2 |
| `jtf evaluate` | Accuracy, drift, dashboard snapshot | 3 |
| `jtf triage` | AI incident note for a failed step | 5 |

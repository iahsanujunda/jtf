# JEPX Tokyo Forecast

Day-ahead forecasts of Tokyo-area solar output and JEPX Tokyo-area prices, for the 48 half-hour slots of the next day, built on public data. Point-in-time correctness is the core: every input carries when it became known, and a time machine rebuilds any past forecast exactly.

## Docs

- [PRD](docs/prd.md): design, data sources, architecture, workflow, models, fault scenarios
- [Phase 1 plan](docs/phase1/README.md): scope, data, order of work, open decisions
- [Fault scenarios](docs/scenarios/README.md): write-ups of planned failures

## Repository layout

| Path | What lives there | From phase |
| --- | --- | --- |
| [jtf/](jtf/) | The `jtf` Python package: all logic outside SQL, the Dagster definitions, the CLI, and its unit tests | 1 |
| [warehouse/dbt/](warehouse/dbt/) | dbt-bigquery project: ODS → DWD → DWS → ADS, monitoring, with its dbt tests and SQL rules | 1 |
| [e2e_tests/](e2e_tests/) | Only what crosses components: the time machine and one day end to end | 1 |
| [notebooks/](notebooks/) | Exploration only; reads dev tables | 1 |
| [deploy/](deploy/) | Everything GCP-specific: settings contract, Terraform, Dagster deployment, scripts | 1 |
| [.github/workflows/](.github/workflows/) | CI, release and model release | 1 |
| [dashboard/](dashboard/) | React dashboard reading daily JSON snapshots, with its Vitest tests | 3 |
| [docs/](docs/) | PRD, phase plans, scenario write-ups, diagrams | – |

Each component owns its code and its tests: `jtf/tests` for Python, dbt tests and SQL rules in `warehouse/dbt`, Vitest in `dashboard`, `terraform validate` and `plan` in `deploy`. `e2e_tests` covers only the guarantees that need several components together. The root `pyproject.toml` is a uv workspace with `jtf` as its member.

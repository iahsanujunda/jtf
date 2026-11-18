# JEPX Tokyo Forecast

Day-ahead forecasts of Tokyo-area solar output and JEPX Tokyo-area prices, for the 48 half-hour slots of the next day, built on public data. Point-in-time correctness is the core: every input carries when it became known, and a time machine rebuilds any past forecast exactly.

## Repository layout

| Path | What lives there |
| --- | --- |
| [jtf/](jtf/) | The `jtf` Python package: all logic outside SQL, the Dagster definitions, the CLI, and its unit tests |
| [warehouse/dbt/](warehouse/dbt/) | dbt-bigquery project: ODS → DWD → DWS → ADS, with DIM beside them for what never changes, and monitoring; its dbt tests and SQL rules |
| [e2e_tests/](e2e_tests/) | Only what crosses components: the time machine and one day end to end |
| [notebooks/](notebooks/) | Exploration only; reads dev tables |
| [deploy/](deploy/) | Everything GCP-specific: settings contract, Terraform, Dagster deployment, scripts |
| [.github/workflows/](.github/workflows/) | CI, release and model release |
| [dashboard/](dashboard/) | React dashboard reading daily JSON snapshots, with its Vitest tests |
| [docs/](docs/) | PRD, phase plans, how a change travels, scenario write-ups, diagrams |

Each component owns its code and its tests: `jtf/tests` for Python, dbt tests and SQL rules in `warehouse/dbt`, Vitest in `dashboard`, `terraform validate` and `plan` in `deploy`. `e2e_tests` covers only the guarantees that need several components together. The root `pyproject.toml` is a uv workspace with `jtf` as its member.

## Working on it

[docs/workflow.md](docs/workflow.md) is how a change travels, from a notebook to the running pipeline: where to work, how a change is proven, how it is released and what to check after. It is the standard for everyone, people and agents alike.

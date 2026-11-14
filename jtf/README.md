# jtf

The Python package behind the `jtf` command, and the owner of all pipeline logic outside SQL. Every pipeline step is a function here. Dagster assets call these functions, the capture and fallback jobs call them through the `jtf` command, and a laptop can do either.

| Subpackage | Holds |
| --- | --- |
| [cli/](src/jtf/cli/) | The `jtf` entry point and its subcommands |
| [orchestration/](src/jtf/orchestration/) | Dagster definitions: assets, partitions, sensors, schedules, checks |
| [ingest/](src/jtf/ingest/) | Fetch and parse, one module per source |
| [storage/](src/jtf/storage/) | Raw and model files by URL (`gs://` in GCP, `file://` in tests) |
| [ml/](src/jtf/ml/) | Baselines, models, training, prediction, backtests and the replay runner |
| [faults/](src/jtf/faults/) | Fault switches and scenario presets |
| [monitoring/](src/jtf/monitoring/) | Checks; later the model health card and the AI triage agent |

Rules that apply everywhere:

- Decision time `T` is always passed in; nothing reads the clock
- Settings come only from environment variables named in [deploy/contract](../deploy/contract/)
- No vendor SDK calls outside `storage/` and the BigQuery client wrapper

## Tests

[tests/](tests/) holds unit tests for this package only: parsers against real source files in `tests/fixtures/`, storage writes being create-only, feature and model code, and Dagster definitions loading. They run offline with `file://` storage and need no GCP credentials. SQL is tested by the warehouse, and guarantees that span components by [e2e_tests](../e2e_tests/).

# dagster

Dagster's own settings, kept here so that they travel with the code.

| File | Holds |
| --- | --- |
| `dagster.local.yaml` | The laptop's instance settings: how long sensor, automation and schedule ticks are kept (skipped ticks one day, the others 30 days). Without it the tick history grows without bound: 2.6 GB in six days. `make dagster` copies it to `.dagster/dagster.yaml` before Dagster starts, since Dagster reads it only then |

Dagster in prod is not built in this phase: it runs on one laptop, which need not be on all the time ([workflow](../../docs/workflow.md#where-you-work)). No machine that has to be patched by hand will hold it; the starting point for the next phase is the daemon as a Cloud Run job per capture window.

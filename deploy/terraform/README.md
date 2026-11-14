# terraform

| Path | Holds | Phase |
| --- | --- | --- |
| `modules/forecast_env/` | Buckets, BigQuery datasets, Artifact Registry, capture and fallback jobs, Scheduler, IAM, Workload Identity Federation, budget | 1 |
| `modules/platform/` | Dagster VM, Cloud NAT, IAP firewall rule, Cloud SQL; MLflow from phase 2. Prod only | 1 |
| `envs/dev/` | The dev project | 1 |
| `envs/prod/` | The prod project: the capture slice, then Dagster | 1 |

State is kept per environment in a Cloud Storage bucket. Cloud SQL edition and tier are pinned explicitly. Everything billable is created here, never by hand.

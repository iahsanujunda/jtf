# terraform

| Path | Holds | Phase |
| --- | --- | --- |
| `modules/forecast_env/` | Buckets, Artifact Registry, capture and fallback jobs, Scheduler, IAM, Workload Identity Federation, budget | 1 |
| `modules/warehouse/` | BigQuery datasets: `ods`, and dbt's layers under a prefix where the environment names one (prod); in dev dbt creates its own, one set per developer and per CI run | 1 |
| `modules/platform/` | Not built. Where the orchestrator runs when it leaves the laptop: decided in the next phase, without a machine to patch | 2 |
| `envs/dev/` | The dev project: where a change is built and tried. Nothing prod needs lives here | 1 |
| `envs/prod/` | The prod project: the capture slice, the warehouse (`ods`, `prod_dim`, `prod_dwd`, `prod_dws`, `prod_ads`), the bucket for trained models' files, and a daily cap on what queries may scan | 1 |

State is kept per environment in a Cloud Storage bucket. Cloud SQL edition and tier are pinned explicitly. Everything billable is created here, never by hand.

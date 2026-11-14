# deploy

Everything GCP-specific; the app knows nothing about where it runs.

| Path | Holds | Phase |
| --- | --- | --- |
| [contract/](contract/) | `settings.example.env`: every variable the image reads, names only | 1 |
| `models.yaml` | Production model version per model; changed only by promotion PRs | 3 |
| [dagster/](dagster/) | Prod Dagster configuration and the VM's services | 1 |
| [terraform/](terraform/) | Modules and per-environment configuration | 1 |
| [scripts/](scripts/) | `promote.sh`: point an environment's jobs at an image digest | 1 |

Tests: `terraform fmt -check`, `terraform validate` and a `terraform plan` per environment on every PR that touches `deploy/`; `docker compose config` for the Dagster services.

How to deploy each environment will be written here as it becomes real. In phase 1, prod gets the capture slice first (raw bucket, Artifact Registry, the capture job and its schedules), then Dagster (VM, Cloud NAT, Cloud SQL).

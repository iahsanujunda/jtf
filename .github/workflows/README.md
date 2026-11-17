# GitHub Actions

| Workflow | Runs on | Does | Phase |
| --- | --- | --- | --- |
| `ci.yml` | Pull requests | One job per component, each running only when its folder changes: `jtf` (ruff, unit tests, `dagster definitions validate`; also when `notebooks/` or `warehouse/dbt/` changes, since ruff lints the notebooks and the tests load the dbt project into Dagster), `warehouse` (dbt build and tests in a PR dataset, SQL rules), `dashboard` (Vitest), `deploy` (`terraform validate` and `plan`); `e2e_tests` when `jtf/` or `warehouse/` changes | 1 |
| `release.yml` | Merge to `main` | Build the image once, deploy to dev, then prod after approval, then post-deploy steps. Phase 1: build, then point the capture job and the Dagster code location at the digest | 1 |
| `model-release.yml` | Change to `deploy/models.yaml` | Move the `production` alias and rewrite the pointer file | 3 |

GitHub reaches GCP only through Workload Identity Federation; no service account keys exist. Backfills are not a GitHub Action: they are launched from the prod Dagster UI.

# Phase 1 progress

**Updated 2026-11-16, 09:50 JST.** Where the work stands, what comes next, and what is waiting on whom. Start here when picking the work back up; update it at the end of every working session. The plan itself is in [README.md](README.md).

## Where things stand

| Step | Status |
| --- | --- |
| 1. Capture | **Done.** 12 Cloud Run jobs in prod: one per scheduled source (10, `meti_fit_capacity` added on 2026-11-16) and the 2 monthly re-captures, each pinging its own healthchecks.io check. The two history-only Open-Meteo sources run as one-off executions of the `openmeteo_live` job |
| 2. Initial history load | **Done except Single Runs.** TEPCO actuals, JEPX, Previous Runs and the demand outlook's archive are loaded. Single Runs: 819 of about 1,370 runs; the rest is waiting on Open-Meteo's daily quota |
| 3. Dagster, locally | **Done.** Raw assets with the bucket sensor, freshness policies, extraction-day partitions, `ods/tepco_actuals` with automation. On 2026-11-16 the automation daemon parsed the 08:45 capture on its own at 08:47 (2 files, 2,225 rows) |
| 5. Parse (first half) | **Done early, plus capacity.** 17 parsers in `jtf/src/jtf/parse/`, each proven on every raw file; all 18 ODS tables loaded in dev BigQuery, the capacity source's three included. Only `tepco_actuals` is orchestrated so far |
| 4, 5 (dbt), 6–9 | Not started |

## Next, in order

1. **2026-11-16 after about 14:30 JST:** resume Single Runs, the last of the initial load (expect about 550 runs, about 165 of them recorded gaps):
   ```
   gcloud run jobs execute jtf-fetch-openmeteo-live --region=asia-northeast1 --project=jepx-tokyo-forecast-prod --task-timeout=3600s --wait --args="fetch,--source,openmeteo_single_runs,--initial,--max-calls,8000"
   ```
   It writes to extraction day 2026-11-16. Parse it with `jtf parse --source openmeteo_single_runs --from 2026-11-16`, or let Dagster do it once Single Runs is orchestrated
2. **Strap the other 16 parsers into Dagster:** add their tables to `ORCHESTRATED` in [orchestration/ods.py](../../jtf/src/jtf/orchestration/ods.py). New captures of every source are then parsed on their own. Days already loaded with `jtf parse` are skipped by the manifest
3. **Step 5 along the solar path first.** The capacity source is live: the `jtf-fetch-meti-fit-capacity` job captures daily at 08:45, its initial load is in the raw bucket (120 files) and parsed into dev BigQuery (`meti_fit_capacity_pref` 291,456 rows, `_city` 10,868,696, `meti_fit_notices` 255; the TEPCO-area figure for March 2026 rebuilt in SQL gives notebook 03's 20.58 GW). Next: notebook 04, versions and `known_at` on real rows of the solar tables; then dbt DWD (with the `known_at` unit tests, the late-backfill case included), the `as_of` macro, DWS solar features and SQL rules in CI. The other tables stay in ODS until a feature needs them
4. **Step 6, the time machine:** replay, and the determinism and future-poison tests
5. **Step 7, the model:** baselines (persistence, TEPCO's forecast), one quantile model, a rolling backtest; skill above persistence on daylight slots
6. **Step 8:** the F2 leak scenario and its write-up, then F1
7. **The release path**, whenever a step finishes early: a `release.yml` that builds the image and points the jobs at its digest, through the `ci-release` service account and WIF (both exist from bootstrap); branch protection on `main`, so that changes reach prod only through a PR as the PRD says; the GitHub Actions version bumps behind the Node 20 warnings; a decision on the image size (now 1.0 GB, see [Known issues](#known-issues))
8. **Step 4, Dagster in prod, last:** the platform module (VM, Cloud SQL, IAP, NAT), the compose deployment, and the `jtf-dagster` heartbeat. Until then, Dagster runs locally against dev BigQuery

## Waiting on you

- **healthchecks.io:** set the cron and grace time of each check once it exists (`jtf-meti-fit-capacity` appears with its first daily ping, on 2026-11-17 at 08:45; 15 minutes' grace): the 9 daily ones (15 minutes' grace for 08:45 sources, 30 for the others), `jtf-tepco-actuals-recapture` (`30 9 1 * *`) and, after its first ping on 1 December, `jtf-tepco-demand-outlook-recapture` (`40 9 1 * *`); delete the old `jtf-morning`, `jtf-jepx` and `jtf-evening`. Skip what is already done

## What runs where

| Piece | Where | State |
| --- | --- | --- |
| GCP projects | `jepx-tokyo-forecast-prod`, `jepx-tokyo-forecast-dev` (Tokyo, dedicated billing account, budget ¥12,000 a month) | Prod: capture, raw bucket, Artifact Registry, Terraform state. Dev: BigQuery only |
| Capture image | `asia-northeast1-docker.pkg.dev/jepx-tokyo-forecast-prod/jtf/jtf@sha256:78265ffb106e…` | Built from commit `e887411` and applied by hand on 2026-11-16 ([How to](#how-to)) |
| Capture schedules (JST) | 08:45: `tepco_actuals`, the three same-day `AREA_*` files, `tepco_demand_outlook`, `openmeteo_live`, `meti_fit_capacity`. 11:00: `jepx_spot`. 19:00 and 22:00: the two next-day files. 09:30 and 09:40 on the 1st: the re-captures | All enabled |
| Raw bucket | `gs://jepx-tokyo-forecast-prod-raw` | 1,005 files (156 MiB) on 2026-11-15, 981 under registered sources; the other 24 are test captures under legacy prefixes, never parsed. Guarded by `prevent_destroy`, 90 days of soft delete and a lien on the project |
| Dagster | `make dagster` on the laptop: UI on http://localhost:3000, state in `.dagster/`, settings in `.env` | Sensors `raw_bucket_sensor` (every 5 minutes) and `automation` running; 11 raw assets, 1 ODS asset |
| Dev BigQuery | `jepx-tokyo-forecast-dev.ods` | 15 tables: `openmeteo_previous_runs` 2.0 M rows, `openmeteo_single_runs` 0.9 M, `tepco_demand_outlook_5min` 487 k, `jepx_spot` 208 k, `tepco_actuals_yearly` 69 k, `tepco_actuals` 53 k (materialized from the Dagster UI), the rest smaller; `raw_files` is the manifest |
| Prod BigQuery | – | Nothing yet: arrives with step 4 |

## Decisions made

Each is recorded where it applies; the list says where.

| Decision | Where |
| --- | --- |
| Capture one source per job; capture outside Dagster, everything rebuildable inside | PRD, phase-1 README |
| Initial loads as one-off executions of the capture jobs; daily previous-month capture and a monthly re-capture for files TEPCO rewrites | Phase-1 README, notebook 01 |
| ODS changes the format, never the meaning; lineage on every row; versions, `known_at` (with `basis`), units and slot times in DWD | Notebook 02, PRD, phase-1 README |
| `known_at` per version: the history rule before a source's capture start, the capture after it | Notebook 02 section 4, phase-1 README |
| Open-Meteo tables are semi-long | Notebook 02 section 6 |
| Every captured file becomes a table: 15 ODS tables; one table per source and file layout | Notebook 02 section 12 |
| The demand outlook's monthly archive since April 2022 is that source's history | Notebook 02 section 12.2, phase-1 README |
| Training and backtests start on target day 2024-02-17: MSM radiation is missing from 23 January to 15 February 2024, and ECMWF radiation starts on 5 March 2024 | Notebook 02 section 8, phase-1 README |
| A parser change appends rows under a new `parser_version`, which is the Dagster code version | Notebook 02, parse README |
| Parsers are proven with `jtf parse` before they are orchestrated | Orchestration code, parse README |
| A Dagster heartbeat to healthchecks.io in step 4 | Phase-1 README, PRD |
| Dagster 1.13.28, pinned; freshness by `FreshnessPolicy.cron`; the automation sensor declared running in code | `jtf/pyproject.toml`, orchestration code |
| The raw bucket is the one thing that can't be rebuilt: Terraform can't destroy it, deleted files are restorable for 90 days, and a lien blocks deleting the project | `deploy/terraform/modules/forecast_env/main.tf` |

## Known issues

- **The image is 1.0 GB** since Dagster, pandas, pyarrow and the BigQuery client joined it. The capture jobs work, but pull a heavy image, and Artifact Registry keeps only one or two releases within the PRD's estimate. Options: a slimmer capture image, or a cleanup policy and accepting it. Decide with the release workflow
- **No release workflow yet.** Images are built on the laptop and applied with Terraform by hand
- **`dagster dev` is superseded by `dg dev`** in Dagster 1.13 (a warning in the log). It works; switching can wait for step 4
- **Freshness counts from when Dagster recorded a capture**, not when it landed: while Dagster is down, a source shows late until the sensor catches up
- **No second copy of the raw bucket.** The guards cover Terraform, deleted files and a deleted project, but not a billing lapse or a lost account. A daily Storage Transfer copy into another project would (156 MiB so far)
- **Data terms not checked yet.** The test fixtures and notebook outputs republish source data in a public repo. Open-Meteo's data is CC BY 4.0, which needs attribution; TEPCO's and JEPX's terms are still to read. Add a data-sources and attribution section with the step-9 README, sooner if the terms require it
- **The next-day forecast is published around 18:50 JST** (the first filled file, for 2026-11-16, says it was written at 18:50:12), so the 19:00 capture is close behind it. Watch whether it ever misses

## Things learned the hard way

| Where | What | Handled by |
| --- | --- | --- |
| Dagster | The default automation sensor starts stopped | Declared in code as running |
| Dagster | Eager automation never runs a partition recorded before the asset existed | The first load of a day is by hand (Materialize or a backfill) |
| BigQuery | `ROWS` is a reserved word | The manifest column is `row_count` |
| BigQuery | A table exists only after its first rows are loaded | Expected: the next-day tables appeared with tonight's first filled files |
| BigQuery client | `load_table_from_dataframe` will need `pandas-gbq` | Added |
| TEPCO | Pages answer 403 to the WebFetch tool | Fetch with `curl` and a user agent |
| TEPCO | This month's archive zip doesn't exist on the 1st (404) | The daily capture takes yesterday's month and the one before |
| TEPCO | Files are UTF-8 or CP932, varying from file to file; archive zips are rewritten for months | Decode by trying UTF-8 first; monthly re-captures |
| Open-Meteo | Connections from Cloud Run sometimes fail to open | The `PATIENT` retry policy and 3 task retries |
| Open-Meteo | Runs missing from the Single Runs archive answer 400 | Stored once as recorded gaps, with `gap: true` in the sidecar |

## How to

```
make setup                 # install the workspace
make test                  # jtf unit tests (offline)
make lint                  # ruff
make dagster               # Dagster UI on http://localhost:3000

# Parse by hand into dev BigQuery (every layout of the source, or one table)
set -a; . ./.env; set +a
uv run jtf parse --source tepco_actuals --from 2026-11-15 [--to 2026-11-16] [--table tepco_actuals_yearly]

# Release an image by hand, until release.yml exists
REPO=asia-northeast1-docker.pkg.dev/jepx-tokyo-forecast-prod/jtf/jtf
TAG=$(git rev-parse --short HEAD)
docker build -t $REPO:$TAG . && docker push $REPO:$TAG
IMAGE=$(docker inspect --format='{{index .RepoDigests 0}}' $REPO:$TAG)
cd deploy/terraform/envs/prod && sed -i "s|^image *=.*|image      = \"$IMAGE\"|" terraform.tfvars
terraform plan -out=prod.tfplan && terraform apply prod.tfplan

# A one-off initial load, as an execution of the source's capture job
gcloud run jobs execute jtf-fetch-<source-slug> --region=asia-northeast1 --project=jepx-tokyo-forecast-prod \
  --task-timeout=1800s --wait --args="fetch,--source,<source>,--initial"
```

Local settings live in gitignored files: `.env` (`JTF_RAW_URL` is the prod raw bucket, read-only from the laptop; `JTF_BIGQUERY_PROJECT` is the dev project), and `terraform.tfvars` in each `deploy/terraform/envs/*`.

## Log

| Date | What happened |
| --- | --- |
| 2026-11-14 | PRD and phase-1 plan; GCP projects, billing and budget; repo skeleton, CI |
| 2026-11-15 morning | Capture in prod, then split to one job per source; the source probe (notebook 00) |
| 2026-11-15 midday | History probe (notebook 01); initial loads; recorded gaps for Single Runs |
| 2026-11-15 afternoon | Notebook 02 and its decisions; step 3, piece 1 (raw assets, sensor, freshness) and piece 2 (dev `ods` dataset, the first parser, the ODS asset) |
| 2026-11-15 evening | Every file as a table (notebook 02 section 12); the demand outlook's archive captured and loaded; all 14 parsers; 14 tables in dev BigQuery |
| 2026-11-16 morning | Guards on the raw bucket (applied); the order of the remaining steps: the solar path through steps 5–8 first, the release path when a step finishes early, Dagster in prod last |
| 2026-11-16 08:47 | Automation parsed the 08:45 capture on its own: step 3 done |
| 2026-11-16 morning | Notebooks end with what the exploration made known, never with decisions (01–03 rewritten) |
| 2026-11-16 09:50 | The capacity source released: image `e887411` on all 12 jobs, the new job and its 08:45 schedule, its initial load (120 files) and parse (18 ODS tables in dev) |
| 2026-11-16 morning | Notebook 03 agreed; the PRD and phase-1 plan updated to match, and the backfill tests written into steps 5 and 6. The capacity source built: capture with an index page, three parsers, tests (89 passing); a real capture into a scratch store took 120 files and the daily one skipped all 66 releases |
| 2026-11-16 morning | Notebook 03, solar capacity: METI's FIT/FIP disclosure verified as the source; the figure is 4.0–8.6 months old when used; the page's own publication dates were revised after the fact |

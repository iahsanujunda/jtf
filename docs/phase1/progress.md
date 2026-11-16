# Phase 1 progress

**Updated 2026-11-16, 17:20 JST.** Where the work stands, what comes next, and what is waiting on whom. Start here when picking the work back up; update it at the end of every working session. The plan itself is in [README.md](README.md).

## Where things stand

| Step | Status |
| --- | --- |
| 1. Capture | **Done.** 12 Cloud Run jobs in prod: one per scheduled source (10, `meti_fit_capacity` added on 2026-11-16) and the 2 monthly re-captures, each pinging its own healthchecks.io check. The two history-only Open-Meteo sources run as one-off executions of the `openmeteo_live` job |
| 2. Initial history load | **Done except Single Runs.** TEPCO actuals, JEPX, Previous Runs and the demand outlook's archive are loaded. Single Runs: 819 of about 1,370 runs; the rest is waiting on Open-Meteo's daily quota |
| 3. Dagster, locally | **Done.** Raw assets with the bucket sensor, freshness policies, extraction-day partitions, `ods/tepco_actuals` with automation. On 2026-11-16 the automation daemon parsed the 08:45 capture on its own at 08:47 (2 files, 2,225 rows) |
| 5. Parse (first half) | **Done early, plus capacity.** 17 parsers in `jtf/src/jtf/parse/`, each proven on every raw file; all 18 ODS tables loaded in dev BigQuery (17 parsed tables, the capacity source's three included, and the `raw_files` manifest). All 17 are Dagster assets since `3de780d`, both extraction days materialized from this laptop's CLI. The machine that runs Dagster has only `ods/tepco_actuals` until it pulls ([tonight](#waiting-on-you-tonight-on-the-machine-that-runs-dagster)); from then on, new captures of every source are parsed on their own |
| 5. Warehouse (started) | **The core and the sun are in.** `dwd_tepco_actuals` (146.8 k versions), `as_of`, the decision-time spine and `dws_solar_features` with D−2 solar; `dwd_sun_slots` (2.3 M rows, nine points × every slot 2016-04 to 2030) and `dwd_daylight` from [notebook 05](../../notebooks/05_clear_sky.ipynb), with F4 as a data test and an SPA reference check. The unit tests, F4 and the SPA check each fail on a deliberate bug; 49 checks green in dev. Not yet: capacity, clear-sky output in MW, the backtest, Dagster, weather, CI. None of the four parts of the plan's step-5 check holds yet: dbt and the `known_at` unit tests in CI; one known day checked by hand against the source files; the lineage in `dagster dev` from raw files to the backtest's metrics; weather known at `T` for every target day |
| 4, 6–9 | Not started |

## Waiting on you: tonight, on the machine that runs Dagster

In this order. Each step says why it sits where it does.

1. **Stop `make dagster`** (Ctrl-C). The pull changes its definitions, and step 5 must run with no Dagster running: a parse appends with no lock, so two parses at once load a file twice.
2. **Update the code:** `git pull` on `main`, then `make setup` (`uv sync`) and `uv sync --group notebook` (for step 9).
3. **Add two lines to `.env`** (dbt needs them; the template is `deploy/contract/settings.example.env`):
   ```
   JTF_BIGQUERY_LOCATION=asia-northeast1
   JTF_DBT_DATASET_PREFIX=dev
   ```
4. **Top up the weather, in prod.** This closes the weather seam at capture start (notebook 04, section 3.4: target day 2026-11-15 has 9 of 24 hours of weather at `T`, 2026-11-16 none). Open-Meteo's daily quota has reset since this morning. Previous Runs first: a new November file up to yesterday, about 50 calls, minutes. Then the rest of Single Runs, the last of the initial load: about 550 runs, about 165 of them recorded gaps, up to an hour. Both run in the cloud, so they need only `gcloud` logged in:
   ```
   gcloud run jobs execute jtf-fetch-openmeteo-live --region=asia-northeast1 --project=jepx-tokyo-forecast-prod --task-timeout=1800s --wait --args="fetch,--source,openmeteo_previous_runs,--initial"
   gcloud run jobs execute jtf-fetch-openmeteo-live --region=asia-northeast1 --project=jepx-tokyo-forecast-prod --task-timeout=3600s --wait --args="fetch,--source,openmeteo_single_runs,--initial,--max-calls,8000"
   ```
   Their files land under the extraction day of the run's JST date: 2026-11-16 if started before midnight, 2026-11-17 after.
5. **Record both days for the 16 new ODS assets, from the CLI, Dagster still stopped.** Automation never runs a day recorded before its asset existed, and this machine's Dagster keeps its own state, so the materializations done on the laptop don't count here. Files already loaded are skipped; the top-up's files are parsed if they landed on 2026-11-16:
   ```
   mkdir -p .dagster && set -a; . ./.env; set +a; export DAGSTER_HOME=$PWD/.dagster
   uv run dagster asset materialize -m jtf.orchestration.definitions --select 'group:ods' --partition 2026-11-15
   uv run dagster asset materialize -m jtf.orchestration.definitions --select 'group:ods' --partition 2026-11-16
   ```
   Every file check should pass. If the top-up landed on 2026-11-17, a day recorded after the assets exist, automation parses it once Dagster runs.
6. **Start `make dagster`.** The sensor catches up on what was captured while it was down (the 19:00 and 22:00 next-day files); those sources show late until it does. From now on, no `jtf parse` while it runs.
7. **Check the load:** the top-up's files are in the manifest, and no file was loaded twice (expect no rows from the second query):
   ```
   bq query --project_id=jepx-tokyo-forecast-dev --use_legacy_sql=false 'select source, extraction_date, count(*) as files, sum(row_count) as row_count from ods.raw_files where source like "openmeteo_%_runs" and extraction_date >= "2026-11-16" group by 1, 2'
   bq query --project_id=jepx-tokyo-forecast-dev --use_legacy_sql=false 'select path, ods_table, parser_version, count(*) as n from ods.raw_files group by 1, 2, 3 having n > 1'
   ```
8. **`make dbt`.** Expect 49 checks green. Proves dbt runs on that machine; the new weather changes nothing in dbt yet (no weather DWD).
9. **Re-run notebook 04** and look at section 3.4: every target day should now have weather at `T`. Leave the notebook uncommitted: its text still describes the gap ([Next](#next-in-order), item 1):
   ```
   uv run --env-file .env --group notebook jupyter nbconvert --to notebook --execute --inplace notebooks/04_ods_to_dwd.ipynb
   ```
10. **Apply `envs/dev`:** `ci-dev` gets `roles/bigquery.user` on the dev project and read access to `ods`, which the CI warehouse job needs. `terraform.tfvars` is gitignored: if that machine has none, copy the example and set `project_id = "jepx-tokyo-forecast-dev"`.
    ```
    cd deploy/terraform/envs/dev
    cp -n terraform.tfvars.example terraform.tfvars
    terraform init -backend-config=bucket=jepx-tokyo-forecast-prod-tfstate
    terraform plan -out=dev.tfplan
    terraform apply dev.tfplan
    ```
    The plan must add exactly two, `google_project_iam_member.ci_dev_bigquery_user` and `google_bigquery_dataset_iam_member.ci_dev_reads_ods`, and change or destroy nothing. Anything else: don't apply, bring the plan to the new session. If `init` adds that machine's platform to `.terraform.lock.hcl`, keep the change for the next commit.
11. **healthchecks.io:** set the cron and grace time of each check once it exists: the 9 daily ones (15 minutes' grace for 08:45 sources, 30 for the others), `jtf-tepco-actuals-recapture` (`30 9 1 * *`); later, `jtf-meti-fit-capacity` after its first ping on 2026-11-17 at 08:45 (15 minutes' grace) and `jtf-tepco-demand-outlook-recapture` (`40 9 1 * *`) after its first on 1 December. Delete the old `jtf-morning`, `jtf-jepx` and `jtf-evening`. Skip what is already done.

Then start the new session with: *read `docs/phase1/progress.md`; tonight's list is done (or: stopped at step N, with this output); pick up Next.*

## Next, in order

1. **Notebook 04 after the top-up:** bring section 3.4's Observed text and the conclusion's lines on the seam up to the re-run's numbers, and commit it (with the lock file, if it changed)
2. **Step 5, the rest of it** ([order of work](README.md#order-of-work)), in this order: the capacity `known_at` probed in SQL in a new notebook 04 section against notebook 03's release table, then `dwd_capacity_releases` and the capacity estimate; clear-sky output in MW and capacity in `dws_solar_features`; the persistence baseline in a rolling backtest, metrics in local MLflow; dbt as Dagster assets; then the weather DWD models with the test that every target day has weather at `T`; the CI warehouse job and the SQL rules (`ci-dev` has its roles after tonight's step 10); one known day checked by hand against the source files. Done when the step-5 check in the plan holds (see [Where things stand](#where-things-stand))
3. **Step 6, the time machine, on the skeleton:** replay, and the determinism, future-poison and backfill-stability tests, in a new `e2e_tests` CI job
4. **Step 7, features and model:** notebook 06 on top of DWS through `as_of` (the label on curtailed slots, clear-sky index, capacity-weighted weather, the quality checks behind F4 and F6); each feature that earns its place becomes a DWS model; the clear-sky × ratio baseline and the TEPCO benchmark; M1 quantile models; skill above persistence on daylight slots
5. **Step 8:** the F2 leak scenario and its write-up, then F1
6. **The release path**, whenever a step finishes early: a `release.yml` that builds the image and points the jobs at its digest, through the `ci-release` service account and WIF (both exist from bootstrap); the `deploy` CI job (`terraform validate` and `plan`); `make bootstrap` (see [Known issues](#known-issues)); branch protection on `main`, so that changes reach prod only through a PR as the PRD says; the GitHub Actions version bumps behind the Node 20 warnings; a decision on the image size (now 1.0 GB, see [Known issues](#known-issues))
7. **Step 4, Dagster in prod, last:** the prod BigQuery datasets and the VM's service account; the platform module (VM, Cloud SQL, IAP, NAT), the compose deployment, the `jtf-dagster` heartbeat, and a one-off backfill of the partitions recorded before the assets existed. Until then, Dagster runs locally against dev BigQuery

## What runs where

| Piece | Where | State |
| --- | --- | --- |
| GCP projects | `jepx-tokyo-forecast-prod`, `jepx-tokyo-forecast-dev` (Tokyo, dedicated billing account, budget ¥12,000 a month) | Prod: capture, raw bucket, Artifact Registry, Terraform state. Dev: BigQuery only |
| Capture image | `asia-northeast1-docker.pkg.dev/jepx-tokyo-forecast-prod/jtf/jtf@sha256:78265ffb106e…` | Built from commit `e887411` and applied by hand on 2026-11-16 ([How to](#how-to)) |
| Capture schedules (JST) | 08:45: `tepco_actuals`, the three same-day `AREA_*` files, `tepco_demand_outlook`, `openmeteo_live`, `meti_fit_capacity`. 11:00: `jepx_spot`. 19:00 and 22:00: the two next-day files. 09:30 and 09:40 on the 1st: the re-captures | All enabled |
| Raw bucket | `gs://jepx-tokyo-forecast-prod-raw` | 1,005 files (156 MiB) on 2026-11-15, 981 under registered sources; the other 24 are test captures under legacy prefixes, never parsed. Guarded by `prevent_destroy`, 90 days of soft delete and a lien on the project |
| Dagster | `make dagster` on one machine only: UI on http://localhost:3000, state in `.dagster/`, settings in `.env` | Runs on the other machine. Until tonight's pull ([Waiting on you](#waiting-on-you-tonight-on-the-machine-that-runs-dagster)) it runs code from before `3de780d`: 11 raw assets and only `ods/tepco_actuals`. Sensors `raw_bucket_sensor` (every 5 minutes) and `automation` running. In code: 17 ODS assets, all 17 materialized for 2026-11-15 and 2026-11-16 from this laptop's CLI, which that machine's state doesn't know about (tonight's step 5). Stopped on this laptop |
| Dev BigQuery | `jepx-tokyo-forecast-dev`: `ods`, and dbt's `dev_dwd` and `dev_dws` (from `make dbt`) | `ods`, 18 tables: `openmeteo_previous_runs` 2.0 M rows and `openmeteo_single_runs` 0.9 M (both grow with tonight's top-up), `tepco_demand_outlook_5min` 487 k, `jepx_spot` 208 k, `tepco_actuals_yearly` 69 k, `tepco_actuals` 53 k, the rest smaller; `raw_files` is the manifest. `dev_dwd`: `dwd_tepco_actuals` 146.8 k versions, `dwd_sun_slots` 2.3 M, `dwd_daylight`, the seeds `weather_points` and `sun_reference`. `dev_dws`: `dws_decision_slots` 48.2 k, `dws_solar_features` |
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
| Every captured file becomes a table: one table per source and file layout (17 now, the capacity source's three included), plus the `raw_files` manifest | Notebook 02 section 12 |
| The demand outlook's monthly archive since April 2022 is that source's history | Notebook 02 section 12.2, phase-1 README |
| Training and backtests start on target day 2024-02-17: MSM radiation is missing from 23 January to 15 February 2024, and ECMWF radiation starts on 5 March 2024 | Notebook 02 section 8, phase-1 README |
| A parser change appends rows under a new `parser_version`, which is the Dagster code version | Notebook 02, parse README |
| Parsers are proven with `jtf parse` before they are orchestrated | Orchestration code, parse README |
| A Dagster heartbeat to healthchecks.io in step 4 | Phase-1 README, PRD |
| Dagster 1.13.28, pinned; freshness by `FreshnessPolicy.cron`; the automation sensor declared running in code | `jtf/pyproject.toml`, orchestration code |
| The raw bucket is the one thing that can't be rebuilt: Terraform can't destroy it, deleted files are restorable for 90 days, and a lien blocks deleting the project | `deploy/terraform/modules/forecast_env/main.tf` |
| One Dagster at a time against dev BigQuery, and no `jtf parse` while it runs | [Known issues](#known-issues) |
| dbt 1.12 with dagster-dbt 0.29.28; datasets named `<prefix>_<layer>` (`dev_dwd`, `dev_dws`); ODS read as dbt sources | `jtf/pyproject.toml`, `warehouse/dbt/README.md` |
| `as_of` is a join condition: `known_at <= t` and `t` before `known_until` | `warehouse/dbt/macros/as_of.sql`, notebook 04 |
| DWD reads each file as last parsed, by `ingested_at`: `parser_version` is a string, so "10" sorts before "9" | `dwd_tepco_actuals.sql`, `models/dwd/README.md` |
| Capacity `known_at` probed in SQL in a notebook 04 section before its model; the 16-day margin stays fixed | Phase-1 README, Decisions |
| Clear-sky output: NOAA sun position and Haurwitz radiation in SQL, geometric (no refraction), the mean of six 5-minute samples per slot; in MW, capacity × the capacity-weighted radiation ÷ 1000 W/m²; checked against an SPA seed | Phase-1 README, Decisions; notebook 05 |
| Night: the sun below 0° at both edges of a slot at all nine points. Every other slot is daylight, the metrics' filter | Phase-1 README, Decisions; `dwd_daylight.sql` |
| Float sums in a fixed order (or rounded): BigQuery's float `SUM` isn't deterministic | Phase-1 README, time machine |
| Weather DWD after the walking skeleton runs | Phase-1 README, Decisions |

## Known issues

- **One Dagster at a time against dev BigQuery.** Each instance keeps its own state, so two would both parse every new capture, and a parse appends without a lock: two runs close together load the same file twice. The same goes for `jtf parse` while Dagster runs. Step 4's single prod instance ends it; until then, a duplicate shows up as a `raw_files` row with the same `path`, `ods_table` and `parser_version`
- **The image is 1.0 GB** since Dagster, pandas, pyarrow and the BigQuery client joined it. The capture jobs work, but pull a heavy image, and Artifact Registry keeps only one or two releases within the PRD's estimate. Options: a slimmer capture image, or a cleanup policy and accepting it. Decide with the release workflow. dbt makes it heavier: `dbt-bigquery` 1.12 requires `google-cloud-aiplatform`, `google-cloud-dataproc` and `nbformat` (for dbt's Python models, which we don't use), so they come with it
- **No release workflow yet.** Images are built on the laptop and applied with Terraform by hand
- **CI has only the `jtf` job.** The plan's CI table also has `warehouse` (dbt build, data and unit tests in a PR dataset, SQL rules), `deploy` (`terraform validate` and `plan`) and `e2e_tests`. `warehouse` comes in step 5, once tonight's apply gives `ci-dev` its roles; `e2e_tests` with step 6's time-machine tests; `deploy` with the release path. Until then, dbt and Terraform are checked only on the laptop
- **No `make bootstrap`.** The plan (Done when, item 8; step 1), the PRD and `deploy/bootstrap/README.md` say both projects are set up through it, but the Makefile has no such target. Add it with the release path, before phase 4's fork guide needs it
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
| BigQuery | `GENERATE_TIMESTAMP_ARRAY` stops at about a million elements | Days and minutes generated apart, then crossed |
| BigQuery | `MOD` takes no floats | `x - n * floor(x / n)` |
| BigQuery | A CTE named like one of its columns returns the whole row as a struct when selected | Different names for CTEs and columns |
| BigQuery | Float `SUM` isn't deterministic: the order of addition varies from run to run | Sums written out in a fixed order, `(s0 + … + s5) / 6` |
| dbt | Unit tests compare exactly, with no tolerance for floats | A reference seed and a singular test with a tolerance (`dwd_sun_slots__matches_spa`) |
| dbt | `dbt-bigquery` 1.12 requires `google-cloud-aiplatform`, `google-cloud-dataproc` and `nbformat` | Accepted; see the image size under [Known issues](#known-issues) |
| uv | Downloads from PyPI time out on a slow line | `UV_HTTP_TIMEOUT=600 uv sync` |

## How to

```
make setup                 # install the workspace
make test                  # jtf unit tests (offline)
make lint                  # ruff
make dagster               # Dagster UI on http://localhost:3000
make dbt                   # dbt build against dev BigQuery (dev_dwd, dev_dws)
make dbt ARGS="build --select dwd_sun_slots+"   # one model and everything after it

# Parse by hand into dev BigQuery (every layout of the source, or one table); never while Dagster runs
set -a; . ./.env; set +a
uv run jtf parse --source tepco_actuals --from 2026-11-15 [--to 2026-11-16] [--table tepco_actuals_yearly]

# Record a day for every ODS asset from the CLI, with Dagster stopped
mkdir -p .dagster && set -a; . ./.env; set +a; export DAGSTER_HOME=$PWD/.dagster
uv run dagster asset materialize -m jtf.orchestration.definitions --select 'group:ods' --partition 2026-11-16

# Run a notebook end to end, outputs saved in place
uv sync --group notebook
uv run --env-file .env --group notebook jupyter nbconvert --to notebook --execute --inplace notebooks/05_clear_sky.ipynb

# The SPA reference seed behind the sun check (pvlib); only if the points or the sample days change
uv run python warehouse/dbt/scripts/write_sun_reference.py

# Dev Terraform (the state bucket comes from bootstrap)
cd deploy/terraform/envs/dev && terraform init -backend-config=bucket=jepx-tokyo-forecast-prod-tfstate
terraform plan -out=dev.tfplan && terraform apply dev.tfplan

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

Local settings live in gitignored files: `.env` (`JTF_RAW_URL` is the prod raw bucket, read-only from the laptop; `JTF_BIGQUERY_PROJECT` is the dev project; `JTF_BIGQUERY_LOCATION` and `JTF_DBT_DATASET_PREFIX` are dbt's; the template is `deploy/contract/settings.example.env`), and `terraform.tfvars` in each `deploy/terraform/envs/*`.

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
| 2026-11-16 midday | The plan's order of work: facts and `as_of` first, a walking skeleton end to end, features graduating one at a time, Dagster in prod last; steps 5 and 7 rewritten to match |
| 2026-11-16 late morning | Notebook 04: DWD in SQL over the solar tables; `as_of` as a range join over all 1,005 decision times; a two-day weather gap found at capture start |
| 2026-11-16 09:50 | The capacity source released: image `e887411` on all 12 jobs, the new job and its 08:45 schedule, its initial load (120 files) and parse (18 ODS tables in dev) |
| 2026-11-16 morning | Notebook 03 agreed; the PRD and phase-1 plan updated to match, and the backfill tests written into steps 5 and 6. The capacity source built: capture with an index page, three parsers, tests (89 passing); a real capture into a scratch store took 120 files and the daily one skipped all 66 releases |
| 2026-11-16 morning | Notebook 03, solar capacity: METI's FIT/FIP disclosure verified as the source; the figure is 4.0–8.6 months old when used; the page's own publication dates were revised after the fact |
| 2026-11-16 midday | Notebook 04's conclusions agreed. All 17 parsers became Dagster ODS assets, with a test that every file of a day is parsed by some asset (90 passing). Both days materialized from the CLI before `dagster dev` started, so automation and the backfill couldn't parse a day at once: only today's `jepx_spot` was new (1 file, 11,088 rows), the rest skipped, every file check passed; automation's first run then loaded nothing. The one raw file no parser reads is `meti_fit_capacity`'s `wayback_cdx.json`, the list of archived copies the capture works from, which is correct |
| 2026-11-16 afternoon | Step 5 started. Clear-sky output, weather DWD after the skeleton and the capacity probe decided (phase-1 plan, Decisions). dbt 1.12 with dagster-dbt 0.29.28; `dwd_tepco_actuals`, `as_of`, the spine and D−2 solar in dev; the `known_at` and `as_of` unit tests each caught a deliberate bug. CI's BigQuery access written in `envs/dev`, applied tonight |
| 2026-11-16 late afternoon | Notebook 05, clear sky: the SQL sun within 0.015° of SPA; night is the sun below 0° at both edges of a slot at all nine points, with no TEPCO output in any of 22,735 such slots; TEPCO's labels confirmed as slot starts by clear days' timing; six samples per slot instead of one, agreed, with float sums in a fixed order. The features notebook becomes 06 |
| 2026-11-16 17:05 | Notebook 05's models: the `weather_points` seed (a pytest keeps it equal to `WEATHER_POINTS`), the `sun_position` macro, `dwd_sun_slots`, `dwd_daylight`, an SPA reference seed with its data test, and F4. 49 dbt checks green; F4 and the SPA check each caught their deliberate bug |
| 2026-11-16 17:20 | Handover checked against the plan: tonight's list for the machine that runs Dagster, the four unmet parts of step 5's check, the missing CI jobs and `make bootstrap`; the day's decisions and lessons recorded |

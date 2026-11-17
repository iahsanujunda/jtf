# Phase 1 progress

**Updated 2026-11-17, 23:30 JST.** Where the work stands, what comes next, and what is waiting on whom. Start here when picking the work back up; update it at the end of every working session. The plan itself is in [README.md](README.md).

## Where things stand

| Step | Status |
| --- | --- |
| 1. Capture | **Done.** 12 Cloud Run jobs in prod: one per scheduled source (10, `meti_fit_capacity` added on 2026-11-16) and the 2 monthly re-captures, each pinging its own healthchecks.io check, on a cron schedule in JST since 2026-11-17 (15 minutes' grace for 08:45 sources, 30 for the others). Since 2026-11-17 17:00 the capacity job also sends a `HEAD` per held release and fetches again any whose `Last-Modified` changed |
| 2. Initial history load | **Done.** TEPCO actuals, JEPX, Previous Runs, Single Runs and the demand outlook's archive. Topped up on 2026-11-16 after capture start, which closed the weather seam ([notebook 04](../../notebooks/04_ods_to_dwd.ipynb), section 3.4): Previous Runs to 15 November, Single Runs complete (1,362 files, runs and recorded gaps; the newest runs are those known at 09:00 on 15 November) |
| 3. Dagster, locally | **Done.** Raw assets with the bucket sensor, freshness policies, extraction-day partitions, ODS assets with automation |
| 5. Parse (first half) | **Done, plus capacity.** 17 parsers, each proven on every raw file, all 17 Dagster assets: new captures of every source are parsed on their own, and both extraction days are recorded on the machine that runs Dagster |
| 5. Warehouse (started) | **The core and the sun are in, run by Dagster; the first automatic chain ran on 2026-11-17.** `dwd_tepco_actuals`, `as_of`, the decision-time spine and `dws_solar_features` with D−2 solar; `dim_sun_slots` and `dim_daylight` ([notebook 05](../../notebooks/05_clear_sky.ipynb)). Since 2026-11-17 the layers are ODS, DIM, DWD, DWS and ADS, and a pytest over dbt's manifest checks that none reads one above it, with one listed exception until `dwd_source_captures`. Since `3c94eb2` dbt's seeds and models are Dagster assets with its data tests as their checks (now 3 seeds, 12 models, 110 data tests and 22 unit tests); the first build from Dagster was green in 33 s, and on 2026-11-17 the morning's captures went through on their own to target day 2026-11-18, all 37 checks green (after a race, fixed: see the [log](#log)). The warehouse rules are reviewed and written down (`f9eff71`). Capacity's `known_at` is probed in SQL ([notebook 04](../../notebooks/04_ods_to_dwd.ipynb), section 6) and built as `dwd_capacity_releases` (notebook 04's dates for all 33 releases, 6 unit tests, 14 checks); its figures as `dwd_capacity_area_files`, the one incremental model, and their versions as `dwd_capacity_area` (notebook 04's figure for all 33 releases, an incremental rebuild equal to a full one, 5 unit tests; section 6.8); and the estimate as `dws_capacity`, one row per decision time (notebook 03's scores over the training window exactly); in `dws_solar_features`, capacity and clear-sky output in MW, weighted by `dws_point_weights`, and the F5 range checks on the actuals. The walking skeleton runs end to end: labels, predictions and outcomes in ADS, and the persistence baseline's rolling backtest in Dagster with its metrics in local MLflow (20 monthly folds, March 2025 to October 2026: MAE 12.4% of capacity on daylight slots, bias −0.10%, 2,537 MW). Not yet: weather DWD, the CI warehouse job and the rest of its rules. Of the four parts of the plan's step-5 check: dbt and the `known_at` unit tests in CI, not yet (`ci-dev` has its roles since 2026-11-17); one known day checked by hand, not yet; the lineage in `dagster dev` runs from raw files to the persistence baseline's backtest metrics, done; weather known at `T` for every target day holds in the data (notebook 04, section 3.4), but the weather models and their test don't exist yet |
| 4, 6–9 | Not started |

## Waiting on you

- **Dagster:** after merging PR #11, reload the code location in the UI before 08:45 on 2026-11-18, so the morning's chain runs with the `dim` keys (PR #10) and the backtest's assets. Restarting `make dagster` also works, and now starts it with MLflow
- **healthchecks.io:** `jtf-meti-fit-capacity` exists since its first ping at 08:45 on 2026-11-17: set cron `45 8 * * *`, Asia/Tokyo, 15 minutes' grace, unless already done. `jtf-tepco-demand-outlook-recapture` appears with its first ping after 09:40 on 1 December: `40 9 1 * *`, 30 minutes. `jtf-tepco-actuals-recapture` is paused (it had gone down under the default one-day period); its ping on 1 December resumes it

## Due on a date

Checks that wait for the calendar, most of them answering what [notebook 04](../../notebooks/04_ods_to_dwd.ipynb) leaves open (its **Still open**). An answer goes into the notebook's *made known* table, and into the plan if it changes a decision.

| When | Check | Answers |
| --- | --- | --- |
| 2026-11-18, after 08:47 | The first chain with the fix for the race of 2026-11-17 (see the [log](#log)): one ODS run, then one run with every model the actuals and the manifest feed together (`dwd_tepco_actuals`, the spine, `dws_capacity`, `dws_point_weights`, `dws_solar_features`, and in ADS `ads_label_solar` and `ads_outcome_solar`), every check green, and no failed run. The capacity job's first scheduled run with the `HEAD` check: `stored=1 unchanged=66` in its log, about 2.5 minutes, its health check green. The machine that runs Dagster must be awake at 08:45 to see it happen on time | The race fix, in the real chain |
| 2026-12-01, after 09:30 | Whether the actuals re-capture added versions to `dwd_tepco_actuals`: which slots, how far the values moved, how long after the slot | How large and how late TEPCO's corrections are |
| Early December, after two weeks of live capture | Re-fetch Single Runs for the days since capture start and match them against the 08:45 live captures: which run was current at 08:45 bounds each model's delay. If the delays allow, the weather DWS takes a fresher run than the older one captured per model and day | The real availability delay of each weather model |
| By mid-December | The release for the end of June 2026: `basis = captured`, `known_at` at the first fetch holding both its files, and no rule-dated release moved | The first captured capacity release |

## Next, in order

1. **Step 5, the rest of it** ([order of work](README.md#order-of-work)), in this order:
   - the weather models, after the hole probe ([notebook 04](../../notebooks/04_ods_to_dwd.ipynb), section 3.6, done): the seed `dim_weather_models`, then one DWD model per source, Previous Runs versioned like every source, each model as the source gave it; then `dws_weather`, a source per target day around capture start (a Single Run only with values in D's daylight) and each hole filled from another model at the same lead, with the strict test that every daylight slot of every target day has weather at `T`
   - `dwd_source_captures` (capture start and newest daily capture per source, so DWS never reads ODS; it ends the one exception the layer rule lists, and gives each backtest run its fidelity, `exact` or `approximate` by decision time, which MLflow doesn't record yet), then the CI warehouse job and the rest of the rules in `warehouse/dbt/rules`, after their two spikes (the `as_of` predicate with sqlglot; the ODS fingerprint)
   - one known day checked by hand against the source files

   Done when the step-5 check in the plan holds (see [Where things stand](#where-things-stand))
2. **Step 6, the time machine, on the skeleton:** replay, and the determinism, future-poison and backfill-stability tests, in a new `e2e_tests` CI job
3. **Step 7, features and model:** plots in each backtest's MLflow run (until then, each metric per fold, which MLflow draws); notebook 06 on top of DWS through `as_of` (the label on curtailed slots, clear-sky index, capacity-weighted weather, the quality checks behind F4 and F6); each feature that earns its place becomes a DWS model; the clear-sky × ratio baseline and the TEPCO benchmark; M1 quantile models; skill above persistence on daylight slots. On the days since capture start, the backtest also measures what serving the live forecast, about a day old, does to the error of a model trained on lead 2, about two days old (notebook 04, Still open)
4. **Step 8:** the F2 leak scenario and its write-up, then F1
5. **The release path**, whenever a step finishes early: a `release.yml` that builds the image and points the jobs at its digest, through the `ci-release` service account and WIF (both exist from bootstrap); the `deploy` CI job (`terraform validate` and `plan`); `make bootstrap` (see [Known issues](#known-issues)); branch protection on `main`, so that changes reach prod only through a PR as the PRD says (CI already runs on pull requests and on pushes to `main`); the GitHub Actions version bumps behind the Node 20 warnings; a decision on the image size (now 1.0 GB, see [Known issues](#known-issues))
6. **Step 4, Dagster in prod, last:** the prod BigQuery datasets and the VM's service account; the platform module (VM, Cloud SQL, IAP, NAT), the compose deployment, the `jtf-dagster` heartbeat, and a one-off backfill of the partitions recorded before the assets existed. Until then, Dagster runs locally against dev BigQuery

## What runs where

| Piece | Where | State |
| --- | --- | --- |
| GCP projects | `jepx-tokyo-forecast-prod`, `jepx-tokyo-forecast-dev` (Tokyo, dedicated billing account, budget ¥12,000 a month) | Prod: capture, raw bucket, Artifact Registry, Terraform state. Dev: BigQuery only |
| Capture image | `asia-northeast1-docker.pkg.dev/jepx-tokyo-forecast-prod/jtf/jtf@sha256:49767fbf9d29…` | Built from commit `75f2b6f` (image tag `c87a62a`, the commit's hash before the history rewrite of 2026-11-17) and applied by hand to all 12 jobs on 2026-11-17 ([How to](#how-to)); the capacity job's smoke run then: the page stored, 66 releases unchanged, none failed |
| Capture schedules (JST) | 08:45: `tepco_actuals`, the three same-day `AREA_*` files, `tepco_demand_outlook`, `openmeteo_live`, `meti_fit_capacity`. 11:00: `jepx_spot`. 19:00 and 22:00: the two next-day files. 09:30 and 09:40 on the 1st: the re-captures | All enabled |
| Raw bucket | `gs://jepx-tokyo-forecast-prod-raw` | 1,686 files (246 MiB with their sidecars) on 2026-11-17; 24 of them are test captures under legacy prefixes, never parsed. Guarded by `prevent_destroy`, 90 days of soft delete and a lien on the project |
| Dagster | `make dagster` on one machine only, the one holding `.dagster/`: UI on http://localhost:3000, settings in `.env` | Code from the checked-out branch, as of the code location's last reload: 12 raw assets, 17 ODS assets with both extraction days recorded, the manifest `ods/raw_files`, dbt's 3 seeds and 12 models with their checks, and the backtest's 2 assets (group `ml`); after a merge that changes assets, reload the code location in the UI. Sensors `raw_bucket_sensor` (every 5 minutes) and `automation` running. Never on a second machine at the same time ([Known issues](#known-issues)) |
| Dev BigQuery | `jepx-tokyo-forecast-dev`: `ods`, and dbt's `dev_dim`, `dev_dwd`, `dev_dws` and `dev_ads` | `ods`, 18 tables: `openmeteo_previous_runs` 2.0 M rows, `openmeteo_single_runs` 1.3 M, `meti_fit_capacity_city` 10.9 M, `tepco_demand_outlook_5min` 500 k, `jepx_spot` 220 k, `tepco_actuals_yearly` 69 k, `tepco_actuals` 55 k, the rest smaller; `raw_files` is the manifest. `dev_dim`: `dim_sun_slots` 2.3 M, `dim_daylight`, the seeds `dim_weather_points`, `dim_tepco_area_parts` and `dim_sun_reference`. `dev_dwd`: `dwd_tepco_actuals` 146.9 k versions, the capacity releases and figures. `dev_dws`: `dws_decision_slots` and `dws_solar_features`, 48.3 k each, `dws_capacity`, `dws_point_weights`. `dev_ads`: `ads_label_solar` 49.0 k slots; `ads_prediction_solar` and `ads_outcome_solar`, 29.3 k rows per backtest run. `ci-dev` can run queries in the project and read `ods` (`envs/dev`, applied 2026-11-17) |
| Prod BigQuery | – | Nothing yet: arrives with step 4 |
| MLflow | Local: `mlflow.db` and `mlruns/` at the repository root, gitignored. UI: `uv run --group ml mlflow ui --backend-store-uri sqlite:///mlflow.db` | Experiment `solar-backtest`: the persistence baseline's first run, 2026-11-17 |

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
| dbt 1.12 with dagster-dbt 0.29.28; datasets named `<prefix>_<layer>` (`dev_dim`, `dev_dwd`, `dev_dws`); ODS read as dbt sources | `jtf/pyproject.toml`, `warehouse/dbt/README.md` |
| `as_of` is a join condition: `known_at <= t` and `t` before `known_until` | `warehouse/dbt/macros/as_of.sql`, notebook 04 |
| DWD reads each file as last parsed, by `ingested_at`: `parser_version` is a string, so "10" sorts before "9" | `dwd_tepco_actuals.sql`, `models/dwd/README.md` |
| Capacity `known_at` in SQL: reproduces notebook 03 for all 33 releases; the history rule reads only copies of the page taken before capture start; the 16-day margin stays fixed; the trend takes the newest releases by quarter, never by `known_at`; three models: dates, each file's figures (incremental: only the files not held at their newest parse, written into the SQL as constants, the only filter BigQuery prunes by) and their versions, rebuilt in full; sums exact | Phase-1 README, Decisions; notebook 04, sections 6 and 6.8 |
| Clear-sky output: NOAA sun position and Haurwitz radiation in SQL, geometric (no refraction), the mean of six 5-minute samples per slot; in MW, capacity × the capacity-weighted radiation ÷ 1000 W/m²; checked against an SPA seed | Phase-1 README, Decisions; notebook 05 |
| Night: the sun below 0° at both edges of a slot at all nine points. Every other slot is daylight, the metrics' filter | Phase-1 README, Decisions; `dim_daylight.sql` |
| Float sums in a fixed order (or rounded): BigQuery's float `SUM` isn't deterministic | Phase-1 README, time machine |
| Weather DWD after the walking skeleton runs | Phase-1 README, Decisions |
| dbt models and seeds are Dagster assets keyed `<layer>/<name>`, dbt's data tests their checks (a test that reads two models names its own in `meta.dagster.ref`). Models rebuild when any input updates (`rebuild_on_update`), not with `eager()`; seeds only by hand | `jtf/src/jtf/orchestration/dbt.py`, orchestration README |
| ADS holds outputs in families: `ads_label_*` (latest actuals, never `as_of`), `ads_prediction_*` (Python, per run), `ads_outcome_*` (dbt, the error defined once), `ads_feature_*` (phase 2 snapshots, never read by models). Backtests write to the dev ADS dataset under their run ID until phase 2 publishes a forecast; `ads_runs` comes with step 6. A fold is a calendar month of target days; metrics on labelled daylight slots, as shares of the capacity at `T` | Phase-1 README, ADS and point-in-time rule 4; `warehouse/dbt/models/ads/README.md` |
| The manifest `ods/raw_files` is owned by no asset: every ODS parse that writes it reports a materialization on it | `jtf/src/jtf/orchestration/ods.py` |
| Weather around capture start: a source per target day (lead 2, a Single Run that covers D, the live capture); Previous Runs versioned like every source. Holes are filled in DWS from another model at the same lead, which always has the hour at lead 2; DWD keeps one model per row as the source gave it | Phase-1 README, Decisions (Weather DWD, Training window); notebook 04, sections 3.2, 3.4 and 3.6 |
| Warehouse rules: `known_at` right per source and every input through `as_of` carry the rest. DWS never reads ODS (`dwd_source_captures` for the spine); what has no `known_at` is its own layer, DIM, which reads only itself; no layer reads one above it, and only DWD reads ODS (a pytest over dbt's manifest); a model with no `known_at` that only its versions may read is private to a dbt group; labels outside DWS; a fingerprint test, not a SQL rule, for overwritten ODS rows | Phase-1 README, Point-in-time rules and Decisions; `warehouse/dbt/rules/README.md` |

## Known issues

- **One Dagster at a time against dev BigQuery.** Each instance keeps its own state, so two would both parse every new capture, and a parse appends without a lock: two runs close together load the same file twice. The same goes for `jtf parse` while Dagster runs. Step 4's single prod instance ends it; until then, a duplicate shows up as a `raw_files` row with the same `path`, `ods_table` and `parser_version`
- **The image is 1.0 GB** since Dagster, pandas, pyarrow and the BigQuery client joined it. The capture jobs work, but pull a heavy image, and Artifact Registry keeps only one or two releases within the PRD's estimate. Options: a slimmer capture image, or a cleanup policy and accepting it. Decide with the release workflow. dbt makes it heavier: `dbt-bigquery` 1.12 requires `google-cloud-aiplatform`, `google-cloud-dataproc` and `nbformat` (for dbt's Python models, which we don't use), so they come with it
- **No release workflow yet.** Images are built on the laptop and applied with Terraform by hand
- **CI has only the `jtf` job.** The plan's CI table also has `warehouse` (dbt build, data and unit tests in a PR dataset, the rules), `deploy` (`terraform validate` and `plan`) and `e2e_tests`. `warehouse` comes in step 5 (`ci-dev` has its roles since 2026-11-17); `e2e_tests` with step 6's time-machine tests; `deploy` with the release path. Until then, dbt and Terraform are checked only on the laptop
- **No `make bootstrap`.** The plan (Done when, item 8; step 1), the PRD and `deploy/bootstrap/README.md` say both projects are set up through it, but the Makefile has no such target. Add it with the release path, before phase 4's fork guide needs it
- **`dagster dev` is superseded by `dg dev`** in Dagster 1.13 (a warning in the log). It works; switching can wait for step 4
- **Freshness counts from when Dagster recorded a capture**, not when it landed: while Dagster is down, a source shows late until the sensor catches up
- **No second copy of the raw bucket.** The guards cover Terraform, deleted files and a deleted project, but not a billing lapse or a lost account. A daily Storage Transfer copy into another project would (156 MiB so far)
- **Data terms not checked yet.** The test fixtures and notebook outputs republish source data in a public repo. Open-Meteo's data is CC BY 4.0, which needs attribution; TEPCO's and JEPX's terms are still to read. Add a data-sources and attribution section with the step-9 README, sooner if the terms require it
- **The next-day forecast is published around 18:50 JST and rewritten around 21:50 with the same values** (both days so far; the file's update line says 18:50:12 and 21:50:03 for 16 November, 18:50:16 and 21:50:02 for the 17th), so each capture runs about 10 minutes behind a write. Watch whether one ever misses
- **Cloud Scheduler can start a job twice.** At 22:00 on 2026-11-16 the scheduler made two attempts for `jtf-fetch-tepco-area-forecast-next-day`, 30 seconds apart and both answered, so the job ran twice and stored two identical files. Harmless: a capture seen again adds no version, and ODS keeps both copies by design
- **Dagster on a laptop stops while the laptop sleeps.** On 2026-11-17 the machine suspended at 01:33 and woke at 10:12, so the 08:45 captures were parsed at 10:17 instead of 08:47. Nothing is lost: capture runs in prod, and the sensor records whatever it hasn't seen when it wakes. Keep the machine awake around 08:45, or accept the delay until step 4 puts Dagster on a VM
- **Every parse rebuilds `dwd_tepco_actuals` and DWS**, not only the actuals' parse: they read the manifest, which every parse writes (JEPX at 11:00, the next-day files at 19:00 and 22:00, capacity at 08:45). A few extra rebuilds a day, each cheap; `dwd_source_captures` narrows it

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
| BigQuery | Float `SUM` isn't deterministic: the order of addition varies from run to run | Sums written out in a fixed order, `(s0 + … + s5) / 6`; capacity figures summed as `NUMERIC` (an incremental and a full build differed by 5 × 10⁻¹⁰ kW on 8 of 297 figures before) |
| BigQuery | Partitions and clustered blocks are pruned only by constants: picking out one file through a subquery or a join read 1,578 MB, more than reading everything (784 MB) | dbt looks the files up first and writes them in (macro `only_files`; notebook 04, section 6.8) |
| dbt-bigquery | An incremental merge reports only the merge's bytes; the read happens in the temporary table it builds first | Costs read from the job history (`INFORMATION_SCHEMA.JOBS_BY_USER`) |
| dbt | A unit test renders `this` and sources as fixtures, so a `run_query` lookup can't run in one; an incremental model's tests must set `is_incremental`, and `this` as a SQL fixture needs no table | `only_files` adds nothing in a unit test; the model filters on the same list in SQL |
| `bq` CLI | Without `--project_id`, a query runs, and is billed, in gcloud's default project, even on fully qualified tables | Pass `--project_id` |
| dbt | Unit tests compare exactly, with no tolerance for floats | A reference seed and a singular test with a tolerance (`dim_sun_slots__matches_spa`) |
| dbt | `dbt-bigquery` 1.12 requires `google-cloud-aiplatform`, `google-cloud-dataproc` and `nbformat` | Accepted; see the image size under [Known issues](#known-issues) |
| uv | Downloads from PyPI time out on a slow line | `UV_HTTP_TIMEOUT=600 uv sync` |
| Dagster | `eager()` on an unpartitioned asset waits until every partition of its partitioned parents exists, so one day with no capture blocks it for good, re-parses included | `rebuild_on_update` for the dbt models, pinned by a test with a never-parsed day |
| Dagster | A table every asset writes and none owns (the manifest) never updates in Dagster's eyes, so nothing downstream of it runs | Each parse that writes it reports a materialization on it |
| Dagster | `materialize` checks partition keys against the real clock | Automation tests use past partitions |
| dagster-dbt | A singular test that reads two models attaches to neither asset | `meta.dagster.ref` names its model |
| Open-Meteo | Previous Runs' lead 0 for the hours just before a fetch changes in a later fetch; leads 1 and 2 don't | Versioned like every source ([notebook 04](../../notebooks/04_ods_to_dwd.ipynb), section 3.4) |
| healthchecks.io | A check that has gone down stays down until its next ping, even after its schedule changes | Pause it: the next ping resumes it |
| Dagster | A model that reads the manifest rebuilt after a parse's first step, while later steps still ran: the spine moved to a new target day ahead of the DWD it is joined with, and `dws_solar_features__d2_complete` failed until the next rebuild a minute later | The manifest is declared downstream of every ODS asset, and `rebuild_on_update` waits while an input's own inputs are being built; a test reproduces the race and fails without the fix |

## How to

```
make setup                 # install the workspace
make test                  # jtf unit tests (offline)
make lint                  # ruff
make dagster               # Dagster UI on http://localhost:3000
make dbt                   # dbt build against dev BigQuery (dev_dim, dev_dwd, dev_dws)
make dbt ARGS="build --select dim_sun_slots+"   # one model and everything after it

# The persistence backtest, as in the UI: the three assets together (folds from run config, default
# March 2025 to the newest month fully labelled), then its run in MLflow
mkdir -p .dagster && set -a; . ./.env; set +a; export DAGSTER_HOME=$PWD/.dagster
uv run --group ml dagster asset materialize -m jtf.orchestration.definitions --select "ads/ads_prediction_solar,ads/ads_outcome_solar,ml/solar_backtest_metrics"
uv run --group ml mlflow ui --backend-store-uri sqlite:///mlflow.db

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
| 2026-11-16 21:30 | On the machine that runs Dagster: pulled, `.env` extended, the weather topped up in prod (Previous Runs 1–15 November, the rest of Single Runs: 368 runs and 175 recorded gaps, 3,487 calls). Dagster, restarted on the new code, parsed both as they landed; no file loaded twice. Both days recorded for the 16 new ODS assets; `make dbt` green there |
| 2026-11-16 22:00 | The 22:00 next-day job ran twice (Cloud Scheduler); TEPCO's file was rewritten at 21:50 with the same values |
| 2026-11-16 late evening | The warehouse rules reviewed as hypotheses against the evidence (`f9eff71`). dbt in Dagster (`3c94eb2`): `eager()` found, offline with Dagster's own evaluator, to block for good on one missing day, replaced by `rebuild_on_update`; the manifest reported by every parse; the first build from the UI green, 37 checks in 33 s; 94 tests passing, also with no dbt manifest or settings, as in CI |
| 2026-11-17 00:30 | Notebook 04 re-run (`a246173`): the seam closed, with a source per target day around capture start; the newest Single Run checked over 187 days (on 9 November MSM's ends before D begins); Previous Runs' lead 0 changes between fetches. Merged to `main`, CI green. `envs/dev` applied (2 added). healthchecks.io on JST crons; the actuals re-capture check paused until its next ping |
| 2026-11-17 10:12 | The machine had slept since 01:33, so the 08:45 captures waited in the bucket. On waking, the sensor recorded them and automation ran the whole chain: one ODS run of 12 assets at 10:17 (every new file parsed, capacity's first daily capture among them), then the spine and features at 10:18, which failed `dws_solar_features__d2_complete`: the spine had rebuilt after the parse's first step reported the manifest, ahead of `dwd_tepco_actuals`. The DWD rebuild at 10:18 and the features' at 10:19 healed it: target day 2026-11-18 complete, all 37 checks green |
| 2026-11-17 10:45 | The race fixed: the manifest downstream of every ODS asset, and `rebuild_on_update` waits while an input's own inputs are being built; a test reproduces the race (96 passing) |
| 2026-11-17 midday | The capacity probe (notebook 04, section 6): one 0.3 MB query reproduces notebook 03's dates for all 33 releases; a later copy of the page once moved a date 151 days after the fact, so the history rule reads only copies before capture start; end of June 2017 is an `.xlsx` never announced, known by the margin on 2024-02-23, so the trend orders by quarter; a re-upload keeps its link, so the capture misses it; values cost 2,085 MB a scan, so dates and values become two models. Sections 1–3 brought up to this morning's data |
| 2026-11-17 14:15 | Notebook 04's open items given dates in [Due on a date](#due-on-a-date); TEPCO corrections, the first captured capacity release and the weather delays are checked when their data exists, the live forecast's freshness in step 7's backtest. METI's file server checked: HEAD gives `Last-Modified`, `If-Modified-Since` is ignored, which shapes the re-upload fix, now due before the next release |
| 2026-11-17 15:35 | `dwd_capacity_releases` built from notebook 04's query, with one change: a release's dates come from each table's first copy, so a re-fetched file (once the re-upload fix lands) can't move a release dated by the rule. All 33 releases equal the notebook's; 6 unit tests, each shown to fail without the part it guards; 14 checks in Dagster. Notebook 04's section-6 cells had failed lint since the probe: CI's `jtf` job ran only on `jtf/**`, though it lints the notebooks and its tests load the dbt project. Reformatted (outputs re-run and unchanged), and the filter now includes `notebooks/` and `warehouse/dbt/` |
| 2026-11-17 16:21 | Capacity's figures built (notebook 04, section 6.8). The spike: BigQuery prunes only by constants, and the history sits in one partition, so the files are looked up in the manifest and written into the SQL; dbt's own jobs read 0 MB on a quiet run, 120 MB for a new release, 2,264 MB for all files. `dwd_capacity_area_files` (incremental, MW per file and part of the area, the seed `tepco_area_parts`) and `dwd_capacity_area` (versions): notebook 04's figure for all 33 releases; two rebuilds from empty equal the full build exactly, once sums were made exact; 5 unit tests, each shown to fail without what it guards; 4 cross-table checks |
| 2026-11-17 16:45 | `dws_capacity`: the least-squares line through the newest four releases known at `T`, by quarter, with the carried figure beside it, in `NUMERIC`. Over the training window it gives notebook 03's scores exactly: trend +0.33% bias, 0.44% mean error, 2.14% largest; carried −1.93%, 1.93%, 2.86%; largest one-day jumps 1.77% and 1.23%. Two builds equal to the bit. For target day 2026-11-18: 21,039 MW, against 20,577 MW carried. 2 unit tests, each shown to fail with the newest chosen by `known_at`, with `known_until` ignored, with five releases, or with a line through fewer than four |
| 2026-11-17 17:00 | The capture of capacity re-uploads written: a watched file the daily capture holds gets a `HEAD`, and a download only when `Last-Modified` (compared as a time) differs from the newest stored copy's; a `HEAD` without `Last-Modified` fails the capture. Notebook 04, section 6.6: all 66 linked files answer `HEAD` with the stored copy's `Last-Modified`, and `If-Modified-Since` is ignored. 5 tests, each shown to fail without what it guards; a live run against METI into a scratch store: the page stored, 66 unchanged, 147 s. |
| 2026-11-17 17:10 | Released by hand: image `c87a62a` (`sha256:49767fbf9d29…`) on all 12 capture jobs. A run of the capacity job right after: `stored=1 unchanged=66 failed=0`. The re-upload issue is closed; whether a re-upload's figures differ stays open until one happens |
| 2026-11-17 17:35 | Clear sky in MW: `dws_point_weights` (each point's share of the newest release known at `T`: mito 0.235 down to numazu 0.033 today) and `dws_solar_features.capacity_mw` and `clear_sky_mw`, exact sums, a rebuild equal on all 48,288 rows. On 16 November, a clear day, actual output runs about 9% above flat-panel clear sky at midday and below it at dawn and dusk: the panel-tilt shape error the plan expects notebook 06 to measure. F5 as data tests: no slot above capacity (highest 0.87), every complete week's peak above a fifth of it (lowest 0.345, week of 2026-09-07, so not the third notebook 03 suggested). Every unit test shown to fail without what it guards, and both F5 checks to fire on the error they guard |
| 2026-11-17 19:00 | History rewritten so every commit carries one author identity, then force-pushed to all branches. The push dropped the merge of PR #8 from `main` (made on GitHub after the local `main` was last pulled); its commit, unchanged, is merged again from `clear-sky-mw`. Every commit since 2026-11-16 has a new hash: the hashes above are updated (`8f61a8a` → `3c94eb2`, `6a96e31` → `f9eff71`, `24d2468` → `a246173`, `c87a62a` → `75f2b6f`); the capture image keeps its tag `c87a62a`, built from the same tree as `75f2b6f` |
| 2026-11-17 19:30 | The DIM layer: `dim_sun_slots`, `dim_daylight` and the three seeds (`dim_weather_points`, `dim_tepco_area_parts`, `dim_sun_reference`) moved out of DWD into `dim`, where nothing above can be read; the `timeless` tag it replaces was never set in code. `dwd_capacity_area_files` is private to the dbt group `capacity_figures`: only `dwd_capacity_area` and the group's two tests can read it. A first rule built, `warehouse/dbt/rules/test_layers.py`: no layer reads one above it, only DWD reads ODS, every model and seed named for its layer, from a fresh `dbt parse`. One exception listed, the spine reading the ODS manifest, which fails the test once it is fixed. Each of the four checks seen to fail. The decision-time spine stays in DWS: it is keyed by decision time, and as DIM it would read DWD. Dev rebuilt: 137 of 137 green, and all 11 tables touched have the same rows and fingerprints as before |
| 2026-11-17 21:45 | Step 5, piece 2: labels, the persistence backtest, MLflow. `ads_label_solar` (latest actuals, delivered + curtailed: 352 curtailed slots, up to 6,243 MW), `ads_prediction_solar` (written by `jtf.ml`, a dbt source in layer `ads`) and `ads_outcome_solar` (the error once, in SQL); the Dagster assets `ads/ads_prediction_solar` and `ml/solar_backtest_metrics`; MLflow 3.18 as the dependency group `ml`, which moved no pinned version. The first run through Dagster in 30 s: 29,280 predictions over 20 monthly folds, one outcome each, every slot labelled; on 15,935 daylight slots, MAE 12.4% of capacity (18.3% in March 2025, 13.4% in October 2026), bias −0.10%, 2,537 MW. Every new test seen to fail on a break of what it guards: six breaks across the four pytests; the two dbt unit tests; labels below delivered, caught on exactly the 352 curtailed slots; labels without the pivot (all 48,977 slots doubled); outcome rows doubled and daylight lost, caught by all three outcome checks. Only `not_null` on the label's key was not broken on its own |
| 2026-11-17 23:30 | Step 5, piece 3 begins with the hole probe (notebook 04, section 3.6): at lead 2, every daylight hole in any weather model is held by another at the same hour (72 point-hours of MSM radiation by ECMWF, 711 of MSM's cloud cover, temperature and wind by GSM and ECMWF, 720 of ECMWF's cloud cover by MSM and GSM), every hole at all nine points at once; the live capture has none; Single Runs' null radiation sits at fixed steps of each run, never in D's daylight. Agreed: the fill lives in DWS, not DWD, and the test that every target day has weather at `T` is strict. Notebook 02 missed that MSM's temperature went too in April 2026 |

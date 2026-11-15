# Phase 1: MVP

**Goal:** a Tokyo-area solar forecast built only from what was known at decision time, a time machine that proves it, and a measured account of what a lookahead leak does to a backtest. Two things run in prod from early on: the capture job, because some source data cannot be fetched later, and Dagster, so every capture and every pipeline run is visible from the start. Feature and model work happens in `dagster dev` on the laptop against the dev project.

This is the working plan for phase 1 and is meant to be complete on its own. The [PRD](../prd.md) holds the overall design and the later phases.

## Done when

1. The capture job runs daily in prod on schedule, and every missed capture raises an alert
2. Dagster runs locally and in prod. The prod UI shows every source's files per day with freshness checks, and the parse and dbt chain runs on its own when new captures land
3. The initial history is loaded, and all phase-1 sources are parsed into ODS, DWD and DWS
4. The time machine passes its determinism and future-poison tests in CI, and a reproduce replay of an earlier what-if run matches it exactly
5. M1 beats persistence (skill above 0) in the rolling backtest on daylight slots, with P10–P90 coverage reported; it is compared with TEPCO's forecast over whatever capture period exists, stated as such
6. F2 (lookahead leak) and F1 (late weather) are written up in [docs/scenarios](../scenarios/)
7. The root README explains the design, the results with their fidelity, and an honest status
8. Nothing account-specific is hard-coded: project IDs, region, billing account and GitHub repo are Terraform variables, and both projects were set up through `make bootstrap`. The fork guide comes in phase 5

## Scope

**In:** the capture job; Dagster locally and in prod, with Cloud SQL as its storage; the dev project; CI; the initial history load; parsers; ODS, DWD and DWS for solar; the as-of macro; the time machine; baselines; M1; the rolling backtest; local MLflow; F1 and F2.

**Out:** the scheduled daily forecast in prod and the fallback job (phase 2), hosted MLflow, the dashboard, monitoring dbt models (drift, accuracy), release gate and shadow runs, the model release flow, M2 (price), AI triage, exports and restore drills, and every fault scenario except F1 and F2.

## Work plan

| # | Step | Deliverables | Done check |
| --- | --- | --- | --- |
| 1 | **Capture, this week** | Repo skeleton (uv, `jtf` CLI, Dockerfile, Makefile, `ci.yml`); `deploy/bootstrap` (APIs, state bucket, WIF, CI service accounts, budget, importing the hand-made budget) behind `make bootstrap`; every account-specific value as a Terraform variable, with `terraform.tfvars.example` files; `storage` module; `jtf fetch` for every source; Terraform for the prod capture slice, one job and schedule per source; `release.yml` that builds the image and points the jobs at its digest; one healthchecks.io check per source | Every source's scheduled run lands files and sidecars in the prod raw bucket; a skipped or failed run alerts on that source's check |
| 2 | Initial history load | `jtf fetch --source S --initial` for the history sources, run as one-off executions of the capture jobs: quota-aware pacing, resumable, tagged `initial`; the daily previous-month capture and the monthly re-capture (see [Learned improvements](#learned-improvements-from-the-history-probe)) | Raw files for every range in [Initial load](#initial-load); a re-run fetches nothing already stored |
| 3 | **Dagster, locally** | `jtf/src/jtf/orchestration`: raw external assets with the bucket sensor, freshness checks per source, partition definitions, the first parse asset (TEPCO actuals) into dev ODS, automation on its downstream; `dagster dev` reading the prod raw bucket read-only | The local UI shows each source's captures per day with freshness checks, and a new capture starts the TEPCO parse on its own |
| 4 | **Dagster, in prod** | Terraform `platform` module: `e2-medium` VM without a public IP, Cloud NAT, IAP firewall rule, Cloud SQL `db-f1-micro` (Enterprise, Tokyo); `deploy/dagster`: `compose.prod.yaml` (webserver, daemon, code location by digest, Cloud SQL Auth Proxy) and `dagster.prod.yaml` (Postgres storage, Docker run launcher, pool limits); `release.yml` also points the code location at the new digest | Through an IAP tunnel, the prod UI shows every capture and the initial load with freshness checks green, and new captures are parsed into prod ODS on their own. A stopped capture shows up both as a failed freshness check and as a healthchecks.io alert |
| 5 | Parse and warehouse | Every parser as an ODS asset; dbt DWD with versions, MW and JST slots; the `as_of` macro; DWS solar features partitioned by decision date; SQL rules in CI. Each piece is built in `dagster dev`, then released, and runs in prod from then on | dbt tests pass; one known day checked by hand against the source files; the prod lineage graph runs from raw files to DWS |
| 6 | Time machine | Replay job (`mode`, `T` range, `fault` in run config) and `jtf replay`; run records in ADS; determinism and future-poison tests | Both tests green in CI |
| 7 | Model | Baselines, the TEPCO benchmark, M1 quantile models, rolling backtest, metrics by segment, MLflow logging; backtest and train jobs launched in `dagster dev` | Skill above 0 vs persistence on daylight slots |
| 8 | Fault scenarios | F2 and F1 run-config presets, replays and write-ups | Write-ups follow the template, with the expected outcome written first |
| 9 | README | Root README (quick start, contributing); design and results with fidelity; status; a LICENSE | Someone new can follow it |

## Data

| Source | Files | Format and units | Phase-1 use |
| --- | --- | --- | --- |
| TEPCO area actuals | `https://www.tepco.co.jp/forecast/html/images/eria_jukyu_YYYYMM_03.csv` (one per month, from 2024-02); older `area-YYYY.csv` (one per fiscal year, FY2016–2023, hourly) UTF-8 or CP932, **varying from file to file**; row 1 `単位[MW平均]`; 30-min slots labelled by start time; **MW averaged over the slot**. The yearly files differ: 万kWh per hour, a two-row header, thousands separators, one thermal column. Columns include `太陽光発電実績`, `太陽光出力制御量` (curtailment), `風力発電実績` | Target, lags |
| TEPCO next-day forecast | `https://www4.tepco.co.jp/forecast/html/images/AREA_ONCE_YOSOKU.csv`, `AREA_ONCE_BGKEI.csv` | Shift_JIS; row 2 holds the file's update time and target date; `時間コマ` 1–48; **kWh per slot**; **wind + solar combined** | Benchmark only |
| TEPCO same-day files | `AREA_YOSOKU.csv`, `AREA_BGKEI.csv`, `AREA_JISEKI.csv` (same folder); `juyo-d1-j.csv` | kWh per slot; 万kW, hourly and 5-min | Captured only, not parsed yet |
| Open-Meteo Previous Runs | `https://previous-runs-api.open-meteo.com/v1/forecast`, `models=jma_msm,jma_gsm`, variables with `_previous_day2` | JSON, hourly | Weather features over the long history |
| Open-Meteo Single Runs | `https://single-runs-api.open-meteo.com/v1/forecast?...&run=YYYY-MM-DDTHH:MM` (UTC) | JSON, hourly; archive from 2026-04-02 | Exact-run features; measuring how pessimistic `previous_day2` is |
| Open-Meteo live | `https://api.open-meteo.com/v1/forecast`, same models and variables | JSON, hourly | Today's forecast inputs |
| JEPX spot summary | Form POST to `https://www.jepx.jp/_download.php` with `dir=spot_summary&file=spot_summary_{FY}.csv` | Shift_JIS; slot codes 1–48; yen/kWh; columns include `エリアプライス東京(円/kWh)` | Price-weighted solar error; ready for M2 |
| Holiday calendar | `jpholiday`, pinned in the image | – | Calendar features |
| Solar capacity estimate | To decide (see [Decisions](#decisions)) | – | Normalising output and error; capacity growth |

**Weather points** (proposed default, one per prefecture in the TEPCO area, refined later): Tokyo (35.69, 139.69), Yokohama (35.44, 139.64), Saitama (35.86, 139.65), Chiba (35.61, 140.12), Mito (36.37, 140.47), Utsunomiya (36.56, 139.88), Maebashi (36.39, 139.06), Kofu (35.66, 138.57), Numazu (35.10, 138.86). Open-Meteo takes all points in one request.

**Variables:** shortwave radiation (GHI), direct and diffuse radiation, cloud cover, 2 m temperature, and wind speed where the model provides it.

Politeness: about 10 requests a day in normal running and no polling loops. Open-Meteo's free tier allows 600 calls a minute and 10,000 a day; the initial load is throttled well below that.

### Capture schedule

Capture is organised **by source**: one source file is one capture job, one raw prefix, one Dagster raw asset and, later, one ODS table. Source names use underscores, because Dagster asset keys and BigQuery tables need them; Cloud Run jobs and health checks swap them for hyphens (`tepco_actuals` runs as `jtf-fetch-tepco-actuals` and pings `jtf-tepco-actuals`). Each source has its own Cloud Run job, its own Cloud Scheduler trigger in the `Asia/Tokyo` time zone, its own retry policy and its own health check, so a slow or failing source never affects another. Capture runs outside Dagster and is the only part of the system that contacts the sources.

| Source | File | Cron (JST) | Retries |
| --- | --- | --- | --- |
| `tepco_actuals` | `eria_jukyu_YYYYMM_03.csv` for the current and the previous month | `45 8 * * *` | standard |
| `tepco_actuals` (re-capture) | Every monthly `eria_jukyu` file, sent with `If-Modified-Since`; only rewritten files are stored, tagged `recapture` | `30 9 1 * *` | standard |
| `tepco_area_forecast` | `AREA_YOSOKU.csv` | `45 8 * * *` | standard |
| `tepco_area_bg_plan` | `AREA_BGKEI.csv` | `45 8 * * *` | standard |
| `tepco_area_actuals_today` | `AREA_JISEKI.csv` | `45 8 * * *` | standard |
| `tepco_demand_outlook` | `juyo-d1-j.csv` | `45 8 * * *` | standard |
| `tepco_area_forecast_next_day` | `AREA_ONCE_YOSOKU.csv`, cleared at about 23:13 and never archived | `0 19,22 * * *` | standard |
| `tepco_area_bg_plan_next_day` | `AREA_ONCE_BGKEI.csv` | `0 19,22 * * *` | standard |
| `openmeteo_live` | One request: all nine points, MSM, GSM and ECMWF IFS 0.25° | `45 8 * * *` | patient: 10 s connect timeout, 5 attempts, 3 task retries, since connections from Cloud Run sometimes fail to open and a fresh attempt succeeds |
| `jepx_spot` | `spot_summary_{FY}.csv` for the current fiscal year | `0 11 * * *` | standard |

*Standard* is a 30 s timeout, 3 attempts on server errors and 1 task retry.

The 08:45 captures run before `T` (09:00), so their `known_at` is at or before `T` and the forecast can use them.

**Health checks:** one healthchecks.io check per source, named `jtf-<source>`. Jobs ping `https://hc-ping.com/<ping-key>/jtf-<source>`: `/start` when they begin, success only when every file was stored, `/fail` with the error otherwise. Only the project's ping key is a secret, and a check that doesn't exist yet is created on its first ping (`?create=1`); its cron and grace time are then set once in the UI: 15 minutes for 08:45 sources, 30 minutes for the others. The monthly re-capture pings its own check, `jtf-tepco-actuals-recapture`; one-off initial loads don't ping.

### Initial load

One-off executions of the capture jobs with `--initial` (and a longer task timeout); Dagster then parses the files like any capture. The numbers come from [the history probe](../../notebooks/01_history_probe.ipynb).

| Source | Range | Requests | Notes |
| --- | --- | --- | --- |
| `tepco_actuals` | Monthly files from 2024-02; yearly files FY2016–FY2023, kept raw | about 42, 8 MB | Final values: later corrections are baked in. The yearly files are parsed only when a later phase needs them |
| `jepx_spot` | FY2016 to last fiscal year | about 10, 20 MB | The current year comes from the daily capture |
| `openmeteo_previous_runs` (history only) | 2024-01 to now, one request per calendar month, all three models, leads `latest`, `previous_day1`, `previous_day2` | about 35, 3,600 Open-Meteo calls | `latest` and `previous_day1` exist only for the leak scenario F2; features use `previous_day2`. MSM's leads exist from mid-2022 to mid-2023 onward, ECMWF's from spring 2024 |
| `openmeteo_single_runs` (history only) | 2026-04-02 to now: MSM 18 and 21 UTC, GSM and ECMWF 12 and 18 UTC, each day | about 1,400, 12,000 calls | More than a day of free quota: the loader paces itself from `X-RateLimit-Cost`, stops cleanly near the daily limit and resumes the next day |
| TEPCO next-day forecast | None exists | – | The benchmark starts at the first capture |

**Loader requirements:** items are generated from name patterns and date ranges; an item already stored under its source is skipped, so a re-run or a resume costs nothing; every file is stored like a daily capture, with `load_type: initial` in its sidecar; `known_at` comes from the history rules at parse time, not at fetch time.

### Learned improvements from the history probe

Each change below came out of [the history probe](../../notebooks/01_history_probe.ipynb) and was confirmed there before being adopted.

| Learned | Evidence | Change |
| --- | --- | --- |
| TEPCO rewrites a month's file for weeks after the month ends | `Last-Modified` dates weeks into the following month | Capture the previous month **every day**, not only on days 1–5 |
| TEPCO occasionally rewrites old files in bulk | February and August 2024 and FY2023 all rewritten on the same day in October 2024 | **Monthly re-capture** of every monthly file on the 1st |
| TEPCO honours `If-Modified-Since` but ignores `If-None-Match` | Confirmation probe: `304` vs a full `200` | The re-capture sends the stored `Last-Modified`; only rewritten files are stored |
| Encoding varies from file to file | UTF-8 for most of 2024–26, CP932 for the newest files and the yearly files | Parsers detect the encoding per file |
| Open-Meteo history costs by size, about 104 calls a month | `X-RateLimit-Cost`, matched by a formula | Quota-aware pacing and a resumable loader |
| Open-Meteo drops the model suffix when one model is requested | Column names in single-model responses | Parsers accept both column forms |
| Lead history is shorter than documented | MSM's leads from mid-2022 to mid-2023, ECMWF's from spring 2024 | Nulls are kept as data; features fall back explicitly |
| Some decision-time runs land exactly at `T` | MSM 21 UTC, GSM and ECMWF 18 UTC with the assumed delays | Two runs per model per day in the single-runs history |
| JMA's single-runs archive starts around mid-May 2026, not 2 April, with scattered missing runs later (ECMWF too, on one day) | The initial load: about 40 runs per JMA run type answered "The requested model run is not available" | A missing run is stored as a recorded gap (the answer and its status in the sidecar) and requested only once; exact-run fidelity for JMA starts in mid-May |

## Point-in-time rules

**Decision time `T` is 09:00 JST on D−1**, one hour before the 10:00 JEPX gate closure. It is one setting, and every run takes `T` explicitly; nothing reads the clock.

1. `known_at` is when we could have had the value: the fetch time from capture start, and the rules below for history. It is never the parse time, so re-parsing keeps it correct
2. A changed value is a new version with its own `known_at`; the same value seen again is not
3. DWS reads inputs only through the `as_of` macro: the latest version with `known_at ≤ T`
4. Targets use the latest values; only inputs go through as-of
5. A value is known from the first capture that contains it; between captures, `known_at` errs late, never early

| Input | `known_at` from capture start | `known_at` for history | Usable at `T` for day D |
| --- | --- | --- | --- |
| TEPCO actuals, one slot | Fetch time of the first file containing that value; each correction is a new version | Slot end + 1 hour | All of D−2; D−1 only before about 08:00. **Lags and persistence use D−2** |
| TEPCO next-day forecast for D | Fetch time on the evening of D−1 | None | Never: published after the gate. Benchmark only |
| Weather, Previous Runs | – | Valid hour − 48 h (`previous_day2`). **`previous_day1` is banned**: for midday slots it comes from a run issued after `T` | All slots of D |
| Weather, Single Runs | – | Run start + assumed delay: MSM 3 h, GSM 6 h, replaced by measured delays | Per slot, the latest run with `known_at ≤ T` that covers the slot |
| Weather, live | Fetch time; run start stored as `source_time` | – | The run fetched at 08:45 |
| JEPX price for day X | Fetch time when first seen | 10:30 JST on X−1 | Up to D−1 |
| Capacity estimate | Publication date of each update | Publication date | The latest update with `known_at ≤ T` |
| Holiday calendar, points, settings | Pinned in the image | Pinned in the image | Whatever the run's image contains |

## Ingestion and warehouse

**Fetch** (`jtf fetch --source S [--initial]`, in the capture jobs only) downloads a file and writes it unchanged through `storage`:

- Path: `raw/{source}/{extraction_date_jst}/{fetched_at}_{filename}`, plus `{…}.meta.json` with fetch time, URL, request parameters, response headers and any update time found inside the file (`source_time`)
- Create-only: `if_generation_match=0` in code, and the job's service account has only `roles/storage.objectCreator` and `roles/storage.objectViewer` (to list and read, for resumable loads and re-captures); neither can delete or overwrite. An existing file cannot be replaced even by a bug
- No parsing in the fetch step

**Parse** runs as Dagster ODS assets (also `jtf parse --source S --from D1 --to D2`) that read raw files and load ODS tables in BigQuery:

| ODS table | From |
| --- | --- |
| `ods.tepco_actuals` | `eria_jukyu` files |
| `ods.tepco_forecast_next_day` | `AREA_ONCE_*` captures |
| `ods.openmeteo_previous_runs` | Previous Runs JSON |
| `ods.openmeteo_single_runs` | Single Runs JSON |
| `ods.openmeteo_live` | Live JSON |
| `ods.jepx_spot` | Spot summary files |

Every ODS row carries `valid_time`, `source_time`, `ingested_at`, `known_at`, `load_type` (`initial` or `daily`), `batch_id`, `source_file` and `parser_version`. Values keep the source's own units and slot labels. ODS is append-only.

**DWD** (dbt) converts power to MW averaged over the slot (kWh per slot ÷ 500; 万kW × 10) and every slot label to its start time in JST; keeps every version as (`valid_time`, `known_at`); keeps weather per run; interpolates hourly weather to 30-minute slots. Range checks: solar ≥ 0, solar = 0 at night (F4), values under installed capacity (F5).

**DWS** (dbt) holds `dws.solar_features` keyed by (`decision_time`, `slot_start`) for every decision time, read only through `as_of`.

**ADS** (dbt and Python) holds `ads.runs` (one row per forecast or replay run: `T`, run ID, mode, image digest, git commit, model version, fallback level, fidelity) and `ads.forecasts` (P10/P50/P90 per slot) in replay datasets. Phase 1 has no published daily forecast yet.

**Warehouse tests** (owned by `warehouse/dbt`): dbt data tests on every model; dbt unit tests for the `as_of` macro, unit conversions and slot labels; SQL rules in `warehouse/dbt/rules` (sqlglot over compiled SQL): no `CURRENT_DATE()`, `CURRENT_TIMESTAMP()` or `NOW()`, DWS models read ODS and DWD only through `as_of`, no `UPDATE` or `DELETE` against ODS.

### Dagster asset graph

Definitions live in `jtf/src/jtf/orchestration`; assets are thin wrappers over `jtf` functions.

| Group | Assets | Partitions | Triggered by |
| --- | --- | --- | --- |
| `raw` (external) | One per source: TEPCO actuals, next-day forecast, same-day files; Open-Meteo live, Previous Runs, Single Runs; JEPX spot | Extraction day (JST) | A sensor on the raw bucket records each landed file against its source and day |
| `ods` | One per parsed source | Extraction day | Declarative automation: new raw records for that day |
| `dwd`, `dws` | dbt models through `dagster-dbt` | DWS by decision date from 2024-02-01; backfills of a range run as one job | Declarative automation: upstream updates |
| `ml` | Backtest and train jobs | – | Launched by hand with run config (phase 1, local) |
| `replay` | Replay job | Decision-date range in run config | Launched by hand |

**Checks:** freshness per source (08:45 sources by 09:00, `jepx_spot` by 11:30, next-day sources by 22:30); completeness (48 slots per day for actuals); units and ranges; dbt tests as asset checks. Every asset carries a code version, so a release that changes one marks its partitions out of date.

**Environments:** `dagster dev` on the laptop uses local storage and runs in-process, reading the prod raw bucket read-only and writing to dev BigQuery. Prod Dagster uses Cloud SQL for storage and runs each run in its own container from the same image.

## Time machine

The replay job (run config: `mode`, `T` range, optional `fault`), also available as `jtf replay --as-of T --mode reproduce|what-if [--from D1 --to D2] [--fault F…]`.

| Mode | Code and model | Expected result |
| --- | --- | --- |
| Reproduce | The image digest and model version recorded in `ads.runs` | Identical to the original run |
| What-if | The current or a candidate image and model | A new run, written to a replay dataset |

What it needs in phase 1: raw files never modified and an append-only ODS; everything that is not data inside the image; every run recorded in `ads.runs` with its input snapshot; determinism (`T` passed in, fixed seeds, LightGBM `deterministic=True` with a fixed `num_threads`, inputs sorted); replays writing only to replay datasets.

**Fidelity:** each run is flagged `exact` (`T` after capture start) or `approximate` (`T` before it). Nearly all phase-1 backtests are `approximate`: JEPX exact, TEPCO actuals slightly optimistic, weather slightly pessimistic (`previous_day2`), no TEPCO benchmark. Results always say which.

**Tests** (in `e2e_tests`, because they need `jtf` and dbt together):

- Determinism: the same `T` twice gives identical output
- Future poison: insert rows with `known_at` after `T` and absurd values; a replay at `T` must not change
- Reproduce: a what-if run, then a reproduce of that run, match exactly

## Model and evaluation

| | M1 solar |
| --- | --- |
| Target | Tokyo-area solar output in MW for each of the 48 slots of D (label on curtailed slots: see Decisions) |
| Inputs | Weather at the points (GHI, direct, diffuse, cloud cover, temperature) from `previous_day2` in history and the live run at `T`; sun position and clear-sky irradiance per slot (pvlib); clear-sky index; capacity estimate; D−2 same-slot output and the median of actual over clear-sky for the last 7 complete days; day of year |
| Model | LightGBM with quantile objectives, one model each for P10, P50 and P90; quantiles sorted if they cross |
| Baselines | Persistence: D−2 same slot. Clear-sky × recent ratio: clear-sky output for D × the 7-day median ratio |
| Benchmark | TEPCO's next-day forecast. It has an unfair advantage (published after the gate) and combines wind and solar, so it is compared with solar + wind actuals, only over the capture period |
| Backtest | Rolling origin over the history from 2024-02, retraining on a fixed cadence, features read as of each decision time; first test month after 12 months of training |
| Metrics | Daylight slots only (sun elevation above 0): MAE normalised by capacity, pinball loss, P10–P90 coverage (target 80%), bias, skill = 1 − MAE(model) / MAE(persistence); price-weighted solar error using the JEPX Tokyo price |
| Segments | Clear vs cloudy days (by clear-sky index), curtailed vs normal days, season |
| Tracking | Local MLflow (`mlflow.db`, gitignored): parameters, git commit, image digest, `T` range, fidelity mix, metrics, plots |

**Fallback ladder** (needed for F1): level 0 is the live run captured at 08:45; level 1 is the previous run that covers D, with ranges widened; level 2 is the clear-sky × ratio baseline. Every forecast records its level.

## Fault scenarios

Run-config presets on the replay job (`fault=F1`, `mode=what-if`), launched from the Dagster UI.

| ID | Injection | Should be caught by | Handling | Write-up shows |
| --- | --- | --- | --- | --- |
| F2 lookahead leak | Two variants: weather from `previous_day0`/`previous_day1` instead of `previous_day2`; and lags read D−1 actuals by bypassing `as_of` | The SQL rule in CI for the bypass; the future-poison test; the gap between leaky and as-of backtests | Block the merge | How much better the leaky backtest looked, per variant and segment |
| F1 late weather | Drop the live run at `T` (and, in replay, the latest qualifying run) | The freshness check on `openmeteo_live` | Fallback ladder level 1, forecast flagged degraded, ranges widened | Accuracy and coverage cost of level 1 and level 2 over the backtest period |

Each write-up uses the [template](../scenarios/README.md), with the expected outcome written before the run.

## Infrastructure and CI

| Where | What | Terraform |
| --- | --- | --- |
| Prod project, Tokyo: capture slice (step 1) | Raw bucket (uniform access, soft delete on); Artifact Registry repository; service account `jtf-fetch` with `roles/storage.objectCreator` and `roles/storage.objectViewer` on the raw bucket only; one Cloud Run job per source (`jtf-fetch-<source>`) pinned to an image digest; one Cloud Scheduler trigger per source and a `jtf-scheduler` service account allowed to run them; a Secret Manager entry for the healthchecks.io ping key | `envs/prod`, `forecast_env` module |
| Prod project, Tokyo: Dagster (step 4) | `e2-medium` VM with no public IP and a 30 GB disk; Cloud NAT; firewall rule allowing only IAP's range; Cloud SQL Postgres `db-f1-micro`, Enterprise edition, no HA, database `dagster`; service account for the VM with BigQuery and read access to the raw bucket | `envs/prod`, `platform` module |
| Dev project, Tokyo | Dev raw bucket for fetch tests; BigQuery datasets `ods`, `dwd`, `dws`, `ads` with per-developer and per-PR prefixes; read access to the prod raw bucket | `envs/dev` |
| Both, via `deploy/bootstrap` | APIs; Terraform state bucket; Workload Identity Federation for GitHub (PRs reach dev; only `main` with the `prod` environment's approval reaches prod); CI service accounts; the budget on the dedicated billing account (¥12,000 a month, alerts at 75%, 100% and on forecast) | `bootstrap` |

**Projects:** `jepx-tokyo-forecast-dev` and `jepx-tokyo-forecast-prod`, standalone (no organization), labelled `app=jtf` and `env=dev|prod`, on a dedicated billing account. The billing account ID stays out of the repo: Terraform reads it from a gitignored `terraform.tfvars`.

**Cost in phase 1:** about $1 a month until step 4 (nine scheduler triggers, six beyond the free tier, add about ¥90); then about $45–50, mostly the VM ($30–35) and Cloud SQL ($10–12). Check the VM price in Google's calculator, since third-party figures disagree.

**Access:** the prod Dagster UI through `gcloud compute start-iap-tunnel` to the VM's webserver port; it is never public.

| Workflow | Phase-1 contents |
| --- | --- |
| `ci.yml` (PRs) | One job per component, each only when its folder changes: `jtf` (ruff, unit tests, `dagster definitions validate`), `warehouse` (dbt build, data and unit tests in a PR dataset, SQL rules), `deploy` (`terraform validate` and `plan`); `e2e_tests` when `jtf/` or `warehouse/` changes |
| `release.yml` (merge to `main`) | Build the image once, push it to Artifact Registry, and after approval point the capture job and the Dagster code location at the new digest |

**Repo skeleton:** Python 3.12; a uv workspace with the root `pyproject.toml` as its root and `jtf/` as its member, each component owning its tests (see the PRD's code and test ownership); `jtf` CLI (Typer); `dagster`, `dagster-webserver`, `dagster-dbt`, `dagster-postgres`, `dagster-docker`, `dagster-gcp`; ruff and pytest; one Dockerfile whose image serves the capture job, the Dagster code location and runs; Makefile for `setup`, `lint`, `test`, `dev` (starts `dagster dev`). Local work authenticates with `gcloud auth application-default login`.

## Decisions

| Decision | Proposed | Needed by step |
| --- | --- | --- |
| Decision time | 09:00 JST on D−1, once the 10:00 gate closure is confirmed in the JEPX trading guide | 1 |
| Weather points and weights | The nine points above, equal weights, refined later | 1 |
| GCP project IDs | Done: `jepx-tokyo-forecast-dev`, `jepx-tokyo-forecast-prod`, on a dedicated billing account | 1 |
| Code license | Apache-2.0 or MIT | 9 |
| Weather models | Done: MSM for solar; ECMWF IFS 0.25° as the radiation fallback (its history starts in spring 2024), because GSM has no radiation through Open-Meteo; GSM kept for cloud cover and temperature | 2 |
| VM size | `e2-medium`; resize if runs need more memory | 4 |
| Dagster UI access | IAP tunnel; Tailscale is the alternative if the tunnel gets tedious | 4 |
| Solar capacity estimate | Candidate: METI's FIT/FIP disclosure of installed capacity by prefecture (to verify) | 5 |
| Label on curtailed slots | Delivered + curtailed (what the weather allowed), keeping delivered as a second column; confirm after looking at the data | 7 |
| Backtest retraining cadence | Monthly | 7 |

## Risks

| Risk | Mitigation |
| --- | --- |
| A missed capture day loses that day's TEPCO next-day forecast for good | Capture runs outside Dagster, one job per source with its own schedule and health check; gaps are recorded as gaps, never filled with later data |
| Setting up prod Dagster takes longer than planned | Step 3 gives the full value for feature work locally; prod Dagster can slip without touching capture, which runs on its own |
| `db-f1-micro` is too small for Dagster (25 connections, 0.6 GB) | Pool sizes capped in `dagster.prod.yaml`; move to `db-g1-small` (about $26 a month) if it struggles |
| A source changes its format or encoding | Raw files kept as received; schema and range checks at parse; fixtures from real files in `jtf/tests/fixtures` |
| The initial load trips a rate limit | Throttled, resumable, one source at a time |
| The TEPCO benchmark has only a few weeks of history by the end of phase 1 | Stated as such in the results; the comparison grows in later phases |
| Phase 1 grows | Anything not in the scope list waits for its phase |

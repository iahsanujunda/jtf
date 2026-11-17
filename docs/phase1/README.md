# Phase 1: MVP

**Goal:** a Tokyo-area solar forecast built only from what was known at decision time, a time machine that proves it, and a measured account of what a lookahead leak does to a backtest. The capture job runs in prod from the start, because some source data cannot be fetched later. Everything after it is rebuilt from the raw files, so it is built in `dagster dev` on the laptop against the dev project, end to end first and richer after, and moves to prod Dagster last (step 4).

This is the working plan for phase 1 and is meant to be complete on its own. The [PRD](../prd.md) holds the overall design and the later phases. Where the work stands right now is in [progress.md](progress.md).

## Done when

1. The capture job runs daily in prod on schedule, and every missed capture raises an alert
2. Dagster runs locally and in prod. The prod UI shows every source's files per day with freshness checks, and the parse and dbt chain runs on its own when new captures land
3. The initial history is loaded, and all phase-1 sources are parsed into ODS, DWD and DWS
4. The time machine passes its determinism and future-poison tests in CI, and a reproduce replay of an earlier what-if run matches it exactly
5. M1 beats persistence (skill above 0) in the rolling backtest on daylight slots, with P10–P90 coverage reported; it is compared with TEPCO's forecast over whatever capture period exists, stated as such
6. F2 (lookahead leak) and F1 (late weather) are written up in [docs/scenarios](../scenarios/)
7. The root README explains the design, the results with their fidelity, and an honest status
8. Nothing account-specific is hard-coded: project IDs, region, billing account and GitHub repo are Terraform variables, and both projects were set up through `make bootstrap`. The fork guide comes in phase 4

## Scope

**In:** the capture job; Dagster locally and in prod, with Cloud SQL as its storage; the dev project; CI; the initial history load; parsers; ODS, DWD and DWS for solar; the as-of macro; the time machine; baselines; M1; the rolling backtest; local MLflow; F1 and F2.

**Out:** the scheduled daily forecast in prod and the fallback job (phase 2), hosted MLflow, the dashboard, monitoring dbt models (drift, accuracy), release gate and shadow runs, the model release flow, M2 (price), AI triage, exports and restore drills, and every fault scenario except F1 and F2.

## Work plan

| # | Step | Deliverables | Done check |
| --- | --- | --- | --- |
| 1 | **Capture, this week** | Repo skeleton (uv, `jtf` CLI, Dockerfile, Makefile, `ci.yml`); `deploy/bootstrap` (APIs, state bucket, WIF, CI service accounts, budget, importing the hand-made budget) behind `make bootstrap`; every account-specific value as a Terraform variable, with `terraform.tfvars.example` files; `storage` module; `jtf fetch` for every source; Terraform for the prod capture slice, one job and schedule per source; `release.yml` that builds the image and points the jobs at its digest; one healthchecks.io check per source | Every source's scheduled run lands files and sidecars in the prod raw bucket; a skipped or failed run alerts on that source's check |
| 2 | Initial history load | `jtf fetch --source S --initial` for the history sources, run as one-off executions of the capture jobs: quota-aware pacing, resumable, tagged `initial`; the daily previous-month capture and the monthly re-capture (see [Learned improvements](#learned-improvements-from-the-history-probe)) | Raw files for every range in [Initial load](#initial-load); a re-run fetches nothing already stored |
| 3 | **Dagster, locally** | `jtf/src/jtf/orchestration`: raw external assets with the bucket sensor, freshness checks per source, partition definitions, the first parse asset (TEPCO actuals) into dev ODS, automation on its downstream; `dagster dev` reading the prod raw bucket read-only | The local UI shows each source's captures per day with freshness checks, and a new capture starts the TEPCO parse on its own |
| 4 | **Dagster, in prod** (last: see [Order of work](#order-of-work)) | The prod BigQuery datasets and a service account for the VM that reads the raw bucket and writes them; Terraform `platform` module: `e2-medium` VM without a public IP, Cloud NAT, IAP firewall rule, Cloud SQL `db-f1-micro` (Enterprise, Tokyo); `deploy/dagster`: `compose.prod.yaml` (webserver, daemon, code location by digest, Cloud SQL Auth Proxy) and `dagster.prod.yaml` (Postgres storage, Docker run launcher, pool limits); `release.yml` also points the code location at the new digest; a heartbeat schedule that pings the healthchecks.io check `jtf-dagster` every 15 minutes, and a run-failure sensor that sends failed runs to its `/fail`; a one-off backfill of the partitions recorded before the assets existed, which automation never runs | Through an IAP tunnel, the prod UI shows every capture and the initial load with freshness checks green, and new captures are parsed into prod ODS on their own. A stopped capture shows up both as a failed freshness check and as a healthchecks.io alert. A stopped Dagster daemon raises the `jtf-dagster` alert within 30 minutes |
| 5 | **Warehouse and walking skeleton** | Every parser as an ODS asset (the parsers themselves were written during step 3, proven on every raw file and loaded with `jtf parse`); the dbt project with DWD for the solar path as [notebook 04](../../notebooks/04_ods_to_dwd.ipynb) designed it (versions, `known_at` with `basis` and `known_until`, MW, JST slots) and capacity as [notebook 03](../../notebooks/03_solar_capacity.ipynb) did, its `known_at` first probed in SQL in a notebook 04 section; the `as_of` macro with the `known_at` unit tests; sun position and clear-sky output per slot in SQL (see [Decisions](#decisions)); a first, minimal DWS (D−2 same-slot output, clear-sky output, the capacity estimate); the persistence baseline scored by a rolling backtest, with metrics in local MLflow; then, once that chain runs, the weather DWD models; all of it as Dagster assets in `dagster dev` against dev BigQuery; SQL rules in CI | `dbt build` and the `known_at` unit tests pass in CI; one known day checked by hand against the source files; in `dagster dev`, the lineage runs from raw files to the persistence baseline's backtest metrics; the weather DWD models pass their tests, every target day with weather known at `T` |
| 6 | Time machine | On the walking skeleton: replay job (`mode`, `T` range, `fault` in run config) and `jtf replay`; run records in ADS; determinism and future-poison tests | The determinism, future-poison and backfill-stability tests green in CI |
| 7 | Features and model | Notebook 06 on top of DWS, read through `as_of`: the label on curtailed slots, clear-sky index, capacity-weighted weather, the data quality behind F4 and F6; each feature that earns its place becomes a DWS model with its tests; the clear-sky × ratio baseline; the TEPCO benchmark; M1 quantile models; metrics by segment; backtest and train jobs launched in `dagster dev` | Skill above 0 vs persistence on daylight slots |
| 8 | Fault scenarios | F2 and F1 run-config presets, replays and write-ups | Write-ups follow the template, with the expected outcome written first |
| 9 | README | Root README (quick start, contributing); design and results with fidelity; status; a LICENSE | Someone new can follow it |

### Order of work

Steps 1–3 are done. The rest runs in this order, not by number:

1. **Step 5**, foundation first: DWD and `as_of` with their tests, then the walking skeleton, the whole chain from raw files to a backtest with the simplest model, as early as possible
2. **Step 6**, the time machine, on the skeleton
3. **Step 7**, features and the model, each feature graduating into DWS
4. **Step 8**, the fault scenarios
5. **The release path**, whenever a step finishes early: `release.yml` (planned in step 1, not built yet), branch protection on `main`, the image's size and cleanup policy. Step 4 needs it
6. **Step 4**, Dagster in prod, once the chain runs end to end in `dagster dev`
7. **Step 9**, the README

### From exploration to production

How a notebook's finding becomes pipeline code, so nothing is written twice and training and serving never diverge:

- **Facts and time first.** DWD (versions, `known_at`, units, slots) and the `as_of` macro are built before any feature, with their tests: a leak there would invalidate every backtest built on top
- **A walking skeleton early.** The whole chain end to end with trivially simple content (three features, the persistence baseline, a backtest), so the pipeline is exercised from the start and the time machine has something to test
- **Explore on top of the pipeline, not beside it.** From notebook 05 on, notebooks read the dbt models through `as_of`, so exploration can't leak either. Exploratory SQL is written the way dbt will run it (notebook 04), so a finding moves into a model without a rewrite
- **Features graduate one at a time.** A feature that earns its place becomes a DWS model with its tests; the notebook keeps the reasoning, the logic lives only in dbt. No feature logic in pandas
- **One feature table.** Backtests read DWS for past decision times, the live forecast reads it for today: one definition, no train/serve skew
- **CI decides.** `dbt build`, its tests and the SQL rules run on every change before anything reaches prod

## Data

| Source | Files | Format and units | Phase-1 use |
| --- | --- | --- | --- |
| TEPCO area actuals | `https://www.tepco.co.jp/forecast/html/images/eria_jukyu_YYYYMM_03.csv` (one per month, from 2024-02); older `area-YYYY.csv` (one per fiscal year, FY2016–2023, hourly) UTF-8 or CP932, **varying from file to file**; row 1 `単位[MW平均]`; 30-min slots labelled by start time; **MW averaged over the slot**. The yearly files differ: 万kWh per hour, a two-row header, thousands separators, one thermal column. Columns include `太陽光発電実績`, `太陽光出力制御量` (curtailment), `風力発電実績` | Target, lags |
| TEPCO next-day forecast | `https://www4.tepco.co.jp/forecast/html/images/AREA_ONCE_YOSOKU.csv`, `AREA_ONCE_BGKEI.csv` | Shift_JIS; row 2 holds the file's update time and target date; `時間コマ` 1–48; **kWh per slot**; **wind + solar combined** | Benchmark only |
| TEPCO same-day files | `AREA_YOSOKU.csv`, `AREA_BGKEI.csv`, `AREA_JISEKI.csv` (same folder); `juyo-d1-j.csv` | kWh per slot; 万kW, hourly and 5-min | Parsed into ODS for feature exploration: the `AREA_*` files with the next-day parser, `juyo-d1-j.csv` into three tables by block shape |
| Open-Meteo Previous Runs | `https://previous-runs-api.open-meteo.com/v1/forecast`, `models=jma_msm,jma_gsm,ecmwf_ifs025`, variables with `_previous_day2` | JSON, hourly | Weather features over the long history |
| Open-Meteo Single Runs | `https://single-runs-api.open-meteo.com/v1/forecast?...&run=YYYY-MM-DDTHH:MM` (UTC) | JSON, hourly; archive from 2026-04-02 | Exact-run features; measuring how pessimistic `previous_day2` is |
| Open-Meteo live | `https://api.open-meteo.com/v1/forecast`, same models and variables | JSON, hourly | Today's forecast inputs |
| JEPX spot summary | Form POST to `https://www.jepx.jp/_download.php` with `dir=spot_summary&file=spot_summary_{FY}.csv` | Shift_JIS; slot codes 1–48; yen/kWh; columns include `エリアプライス東京(円/kWh)` | Price-weighted solar error; ready for M2 |
| Holiday calendar | `jpholiday`, pinned in the image | – | Calendar features |
| Solar capacity (METI's FIT/FIP disclosure) | The summary page `https://www.fit-portal.go.jp/PublicInfoSummary`, and per quarter table A (`A_prefYYYYMM.xlsx`, by prefecture) and table B (`B_cityYYYYMM.xlsx`, by municipality), linked by unguessable file ids | `.xlsx`, six sheets, a three-row header; **kW**; FIT/FIP installations only; published 4 to 6 months after the quarter it describes | Normalising output and error, the F5 range check, the weather-point weights. The TEPCO area is the eight prefectures plus Shizuoka east of the Fuji River ([notebook 03](../../notebooks/03_solar_capacity.ipynb)) |

**Weather points** (one per prefecture in the TEPCO area, each weighted by its prefecture's share of solar capacity as of `T`: northern Kanto holds 72% of it, [notebook 03](../../notebooks/03_solar_capacity.ipynb) section 8): Tokyo (35.69, 139.69), Yokohama (35.44, 139.64), Saitama (35.86, 139.65), Chiba (35.61, 140.12), Mito (36.37, 140.47), Utsunomiya (36.56, 139.88), Maebashi (36.39, 139.06), Kofu (35.66, 138.57), Numazu (35.10, 138.86). Open-Meteo takes all points in one request.

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
| `tepco_demand_outlook` | `juyo-d1-j.csv`, plus the archive zips (`YYYYMM_power_usage.zip`) of yesterday's month and the month before: yesterday's final version lands in its month's zip each morning. Not this month's on the 1st, when it doesn't exist yet | `45 8 * * *` | standard |
| `tepco_demand_outlook` (re-capture) | Every archive zip, sent with `If-Modified-Since`; TEPCO rewrites them for months | `40 9 1 * *` | standard |
| `tepco_area_forecast_next_day` | `AREA_ONCE_YOSOKU.csv`, cleared at about 23:13 and never archived | `0 19,22 * * *` | standard |
| `tepco_area_bg_plan_next_day` | `AREA_ONCE_BGKEI.csv` | `0 19,22 * * *` | standard |
| `openmeteo_live` | One request: all nine points, MSM, GSM and ECMWF IFS 0.25° | `45 8 * * *` | patient: 10 s connect timeout, 5 attempts, 3 task retries, since connections from Cloud Run sometimes fail to open and a fresh attempt succeeds |
| `jepx_spot` | `spot_summary_{FY}.csv` for the current fiscal year | `0 11 * * *` | standard |
| `meti_fit_capacity` | The FIT/FIP summary page, stored every day (its notices date each release), and every table A and B file it links that the bucket doesn't hold yet. The file URLs are unguessable ids, so this capture reads the page for links | `45 8 * * *` | standard |

*Standard* is a 30 s timeout, 3 attempts on server errors and 1 task retry.

The 08:45 captures run before `T` (09:00), so their `known_at` is at or before `T` and the forecast can use them.

**Health checks:** one healthchecks.io check per source, named `jtf-<source>`. Jobs ping `https://hc-ping.com/<ping-key>/jtf-<source>`: `/start` when they begin, success only when every file was stored, `/fail` with the error otherwise. Only the project's ping key is a secret, and a check that doesn't exist yet is created on its first ping (`?create=1`); its cron and grace time are then set once in the UI: 15 minutes for 08:45 sources, 30 minutes for the others. The monthly re-capture pings its own check, `jtf-tepco-actuals-recapture`; one-off initial loads don't ping. From step 4, Dagster pings its own check, `jtf-dagster`: a heartbeat every 15 minutes (grace 15 minutes), and `/fail` with the run ID when a run fails. A Dagster that has stopped can't report itself, so its silence is what alerts, and every alert arrives through one channel.

### Initial load

One-off executions of the capture jobs with `--initial` (and a longer task timeout); Dagster then parses the files like any capture. The numbers come from [the history probe](../../notebooks/01_history_probe.ipynb).

| Source | Range | Requests | Notes |
| --- | --- | --- | --- |
| `tepco_actuals` | Monthly files from 2024-02; yearly files FY2016–FY2023 | about 42, 8 MB | Final values: later corrections are baked in. The yearly files have their own table, `ods.tepco_actuals_yearly` |
| `jepx_spot` | FY2016 to last fiscal year | about 10, 20 MB | The current year comes from the daily capture |
| `tepco_demand_outlook` | TEPCO's archive of the file, `YYYYMM_power_usage.zip`, 2022-04 to yesterday's month: one end-of-day file a day | 56, about 4.5 MB | Archived "tomorrow" figures were updated on the day they describe; their own update times keep them out of day-before features ([notebook 02](../../notebooks/02_raw_to_ods.ipynb), section 12.2) |
| `openmeteo_previous_runs` (history only) | 2024-01 to now, one request per calendar month, all three models, leads `latest`, `previous_day1`, `previous_day2` | about 35, 3,600 Open-Meteo calls | `latest` and `previous_day1` exist only for the leak scenario F2; features use `previous_day2`. MSM's leads exist from mid-2022 to mid-2023 onward; ECMWF's radiation from 5–7 March 2024, its other variables from early February. MSM's radiation is missing at every lead from 23 January to 15 February 2024 (notebook 02) |
| `openmeteo_single_runs` (history only) | 2026-04-02 to now: MSM 18 and 21 UTC, GSM and ECMWF 12 and 18 UTC, each day | about 1,400, 12,000 calls | More than a day of free quota: the loader paces itself from `X-RateLimit-Cost`, stops cleanly near the daily limit and resumes the next day |
| `meti_fit_capacity` | Every `.xlsx` release of tables A and B since the end of June 2017, and the archived copies of the summary page in the Wayback Machine | 66 files, 61 MB; 52 pages, 3 MB | The archived pages are the only record of when older releases were announced; the three `.xls` releases of 2017–2018 are left out ([notebook 03](../../notebooks/03_solar_capacity.ipynb)) |
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
| Lead history is shorter than documented | MSM's leads from mid-2022 to mid-2023, ECMWF's radiation from 5–7 March 2024; MSM's radiation missing from 23 January to 15 February 2024 (notebook 02) | Nulls are kept as data; features fall back explicitly |
| Some decision-time runs land exactly at `T` | MSM 21 UTC, GSM and ECMWF 18 UTC with the assumed delays | Two runs per model per day in the single-runs history |
| JMA's single-runs archive starts on 12 May 2026, not 2 April, with scattered missing runs later (ECMWF too, on one day) | The initial load: about 40 runs per JMA run type answered "The requested model run is not available"; [the history probe](../../notebooks/01_history_probe.ipynb), section 7, pins the start | A missing run is stored as a recorded gap (the answer and its status in the sidecar) and requested only once; exact-run fidelity for JMA starts on 12 May |
| The capture account couldn't list the raw bucket | The initial load: `403` on listing with only `objectCreator` | Add `objectViewer`: reading and listing, still no delete or overwrite |
| A permanent error, counted as a failure, is multiplied by Cloud Run's task retries | The first load: about 1,500 quota calls per retry on runs that can't exist | Only errors a retry can fix fail a source |

## Point-in-time rules

**Decision time `T` is 09:00 JST on D−1**, one hour before the 10:00 JEPX gate closure. It is one setting, and every run takes `T` explicitly; nothing reads the clock.

1. `known_at` is when we could have had the value. A value's first version takes the history rule below when the rule's time falls before the source's capture start (its first daily capture); every other version takes the fetch time of the first capture containing it. `load_type` doesn't decide this, because the first daily capture already carries weeks of history ([notebook 02](../../notebooks/02_raw_to_ods.ipynb), section 4). Each version records its `basis`: `rule` or `captured`. It is never the parse time, so re-parsing keeps it correct
2. A changed value is a new version with its own `known_at`; the same value seen again is not
3. DWS reads versioned DWD models only through the `as_of` macro: the latest version with `known_at ≤ T`. It never reads ODS: anything it needs from ODS gets a DWD model first. DWD models that hold nothing learned over time (the sun's position, daylight, later the holiday calendar) carry no `known_at`; they are tagged timeless and read directly
4. Targets use the latest values; only inputs go through as-of. Labels live in their own models, outside DWS, so that rule 3 holds for every DWS read
5. A value is known from the first capture that contains it; between captures, `known_at` errs late, never early

What each input can use at `T` (the table's last column) follows from rules 1 and 3 and needs no check of its own: the midday hours of `previous_day1`, TEPCO's next-day forecast and most of D−1 carry a `known_at` after `T`, so `as_of` never returns them. The checks guard those two rules: `known_at` right for each source (dbt unit and data tests, built source by source as its evidence comes in), and every input read through `as_of` (the rules in `warehouse/dbt/rules` and the future-poison test).

| Input | `known_at` from capture start | `known_at` for history | Usable at `T` for day D |
| --- | --- | --- | --- |
| TEPCO actuals, one slot | Fetch time of the first file containing that value; each correction is a new version | Slot end + 1 hour | All of D−2; D−1 only before about 08:00. **Lags and persistence use D−2** |
| TEPCO next-day forecast for D | Fetch time on the evening of D−1 | None | Never: published after the gate. Benchmark only |
| Weather, Previous Runs | – | Valid hour − 24 h × N + the model's delay (MSM 3 h, GSM and ECMWF 6 h) for `previous_dayN`; features use `previous_day2`. **`previous_day1` is banned**: for midday slots it comes from a run issued after `T` | All slots of D |
| Weather, Single Runs | – | Run start + assumed delay: MSM 3 h, GSM and ECMWF 6 h, replaced by measured delays | Per slot, the latest run with `known_at ≤ T` that covers the slot |
| Weather, live | Fetch time (the response names no run) | – | The run fetched at 08:45 |
| JEPX price for day X | Fetch time when first seen | 10:30 JST on X−1 | Up to D−1 |
| Solar capacity, one release | Fetch time of the first capture holding both its files | The end of the latest announcement date any copy of the summary page taken before capture start showed (later copies can't move it), and never before the files' `Last-Modified`; a file never announced: `Last-Modified` + 16 days. The page's own dates were later moved earlier, by up to 16 days ([notebook 03](../../notebooks/03_solar_capacity.ipynb), section 2) | The newest release with `known_at ≤ T`, 4 to 9 months old; DWS extends it to D (below) |
| Holiday calendar, points, settings | Pinned in the image | Pinned in the image | Whatever the run's image contains |

## Ingestion and warehouse

**Fetch** (`jtf fetch --source S [--initial]`, in the capture jobs only) downloads a file and writes it unchanged through `storage`:

- Path: `raw/{source}/{extraction_date_jst}/{fetched_at}_{filename}`, plus `{…}.meta.json` with fetch time, URL, request parameters and response headers. The update time written inside some files is read by the parser, as `source_time`
- Create-only: `if_generation_match=0` in code, and the job's service account has only `roles/storage.objectCreator` and `roles/storage.objectViewer` (to list and read, for resumable loads and re-captures); neither can delete or overwrite. An existing file cannot be replaced even by a bug
- No parsing in the fetch step. One narrow exception: `meti_fit_capacity` reads the FIT/FIP summary page for the links to new files, because their URLs are unguessable ids; the page itself is stored like any file

**Parse** runs as Dagster ODS assets (also `jtf parse --source S --from D1 --to D2`) that read raw files and load ODS tables in BigQuery. The parsers live in `jtf/src/jtf/parse/`, one module per file layout, and were designed and run over every raw file in [notebook 02](../../notebooks/02_raw_to_ods.ipynb). **Parsing changes the format, never the meaning:** one ODS row per data row, ASCII column names, typed values, the source's own units and time labels.

| ODS table | From | One row per |
| --- | --- | --- |
| `ods.tepco_actuals` | Monthly `eria_jukyu` files | Slot, per capture |
| `ods.tepco_actuals_yearly` | Yearly `area-YYYY.csv` files, FY2016–FY2023 (万kWh, hourly) | Hour |
| `ods.tepco_area_forecast_next_day`, `ods.tepco_area_bg_plan_next_day` | `AREA_ONCE_YOSOKU`, `AREA_ONCE_BGKEI` | Slot code, per capture |
| `ods.tepco_area_forecast`, `ods.tepco_area_bg_plan`, `ods.tepco_area_actuals_today` | `AREA_YOSOKU`, `AREA_BGKEI`, `AREA_JISEKI`, with the same parser | Slot code, per capture |
| `ods.tepco_demand_outlook_peaks`, `_hourly`, `_5min` | `juyo-d1-j.csv`, by block shape: the headline figures, the hourly block, the 5-minute block (demand and solar) | Figure, hour or 5 minutes, per file |
| `ods.jepx_spot` | Spot summary files | Delivery day and slot code, per capture |
| `ods.openmeteo_previous_runs` | Previous Runs JSON | Point, model, `lead_days`, hour |
| `ods.openmeteo_single_runs` | Single Runs JSON | Point, model, hour, per run (`run_time`) |
| `ods.openmeteo_live` | Live JSON | Point, model, hour, per capture |
| `ods.meti_fit_capacity_pref`, `ods.meti_fit_capacity_city` | FIT/FIP tables A and B, every sheet | Sheet, area (prefecture or municipality) and header path, per file: semi-long |
| `ods.meti_fit_notices` | Every stored copy of the FIT/FIP summary page, live or archived | Announcement (notice line or heading), per page |
| `ods.raw_files` | Every sidecar | Raw file: status (`parsed`, `empty`, `gap`, `failed`), rows, check result |

The Open-Meteo tables are semi-long: the model and lead, which the API packs into column names, become columns, and each variable stays a column with its unit asserted. Nothing captured stays raw: one ODS table per source and file layout (or block shape), 18 tables in all ([notebook 02](../../notebooks/02_raw_to_ods.ipynb), section 12; [notebook 03](../../notebooks/03_solar_capacity.ipynb)). Only prefixes that are not registered sources are ignored.

Every ODS row carries its lineage: `source_file`, `extraction_date`, `fetched_at`, `load_type` (`daily`, `initial` or `recapture`), `source_time` (the update time written in the file, else `Last-Modified`), `row_number` (CSV tables), `parser_version`, `ingested_at` and `batch_id`. ODS has no `known_at` and no shared `valid_time`: a version and its `known_at` depend on other captures of the same file, and slot times are meaning, so DWD derives both. A parser refuses a file whose shape changed (unknown columns, unit row, units, time zone, column count); a missing column becomes nulls. ODS is append-only and partitioned by `extraction_date`: a parser change re-parses under a new `parser_version`, and DWD reads the newest version per file.

**DWD** (dbt) converts power to MW averaged over the slot (kWh per slot ÷ 500, checked against the MW file in notebook 02; 万kW × 10) and every slot label to its start time in JST; derives versions with their `known_at` and `basis` from ODS's lineage, per the point-in-time rules; drops `AREA_JISEKI` slots that end after the file's own update time (it fills them with zeros); keeps weather per run; interpolates hourly weather to 30-minute slots, taking radiation at hour `t` as the mean of the hour before `t`; fills weather holes from ECMWF at the same lead where it has the variable, flagging the row. Builds the capacity releases (`ref_date`, `known_at`, `basis`) from the notices and the manifest, and, apart from them since they cost 2 GB a scan against 0.3 MB, the TEPCO-area figure ([notebook 04](../../notebooks/04_ods_to_dwd.ipynb), sections 6.7 and 6.8), in two models: each raw file's figures per part of the area, incremental, reading only the files not held at their newest parse (their days and paths written into the SQL as constants, the only filter BigQuery prunes by), summed exactly; then their versions, rebuilt in full. The area is the eight prefectures from table A and the 20 Shizuoka municipalities east of the Fuji River from table B, solar only, in MW. Range checks: solar ≥ 0, solar = 0 at night (F4); values under installed capacity, and every complete week's peak above a fifth of it (F5: the ceiling catches kWh read as MW, only the floor catches 万kW read as MW; the lowest real week since February 2024 was 0.345 of capacity, too close to the third notebook 03 suggested). Both read the capacity estimate in DWS.

**DWS** (dbt) holds `dws.solar_features` keyed by (`decision_time`, `slot_start`) for every decision time, read only through `as_of`. The capacity estimate for D is the straight line through the newest four releases known at `T`, by the quarter they describe (carrying the newest figure runs about 2% low; [notebook 03](../../notebooks/03_solar_capacity.ipynb), section 7), with the carried figure kept beside it: `dws_capacity`, one row per decision time, for the features, the metrics' normalisation and the range checks, in exact arithmetic so a decision time always gives the same figure; the weather-point weights are the prefectures' capacity shares at the newest known release (`dws_point_weights`), and clear-sky output in MW is `dws_solar_features.clear_sky_mw`.

**ADS** (dbt and Python) holds `ads.runs` (one row per forecast or replay run: `T`, run ID, mode, image digest, git commit, model version, fallback level, fidelity) and `ads.forecasts` (P10/P50/P90 per slot) in replay datasets. Phase 1 has no published daily forecast yet.

**Warehouse tests** (owned by `warehouse/dbt`): dbt data tests on every model; dbt unit tests for the `as_of` macro, unit conversions and slot labels, and for `known_at`: a late backfill (an initial load fetched after capture start gets its fetch time, never the history rule), capture start per source (a source added later, such as capacity, keeps the rule for its own past), the same value seen again (no new version), a correction (a new version, later), a re-parse under a new `parser_version` (every `known_at` unchanged), and a later copy of the FIT/FIP page showing an earlier date (no earlier `known_at`); after every load, data tests that `basis = rule` rows sit before their source's capture start, that `basis = captured` rows carry the fetch time of a file holding the value, and that no `known_at` lies after the newest fetch; rules in `warehouse/dbt/rules` ([its README](../../warehouse/dbt/rules/README.md)), run in CI: no DWS model depends on an ODS source (read from dbt's manifest); every versioned DWD model a DWS model reads is joined through `as_of` (sqlglot over compiled SQL; timeless models and labels are outside it); no `CURRENT_DATE()`, `CURRENT_TIMESTAMP()` or `NOW()` in any model; and after every load, each file's rows in ODS still match the fingerprint recorded when they were loaded. The last is a data test, not a SQL rule: dbt never writes ODS, and BigQuery has no insert-only role, so a rule over dbt's SQL could not catch an `UPDATE` from anywhere else.

### Dagster asset graph

Definitions live in `jtf/src/jtf/orchestration`; assets are thin wrappers over `jtf` functions.

| Group | Assets | Partitions | Triggered by |
| --- | --- | --- | --- |
| `raw` (external) | One per source: TEPCO actuals, next-day forecast, same-day files; Open-Meteo live, Previous Runs, Single Runs; JEPX spot | Extraction day (JST) | A sensor on the raw bucket records each landed file against its source and day |
| `ods` | One per parsed source | Extraction day | Declarative automation: new raw records for that day |
| `dwd`, `dws` | dbt models through `dagster-dbt` | DWS by decision date from 2024-02-16 (target day 2024-02-17, the first with full MSM radiation); backfills of a range run as one job | Declarative automation: upstream updates |
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

What it needs in phase 1: raw files never modified and an append-only ODS; everything that is not data inside the image; every run recorded in `ads.runs` with its input snapshot; determinism (float sums added in a fixed order or rounded, since BigQuery doesn't promise the same `SUM` of floats twice; `T` passed in, fixed seeds, LightGBM `deterministic=True` with a fixed `num_threads`, inputs sorted); replays writing only to replay datasets.

**Fidelity:** each run is flagged `exact` (`T` after capture start) or `approximate` (`T` before it). Nearly all phase-1 backtests are `approximate`: JEPX exact, TEPCO actuals slightly optimistic, weather slightly pessimistic (`previous_day2`), no TEPCO benchmark. Results always say which.

**Tests** (in `e2e_tests`, because they need `jtf` and dbt together):

- Determinism: the same `T` twice gives identical output
- Future poison: insert rows with `known_at` after `T` and absurd values; a replay at `T` must not change
- Backfill stability: build features as of several past `T`, load a simulated backfill fetched now (raw files for past periods, as a late initial load would bring), and build them again. Every input captured before `T` must come out identical; new rows may appear only with `basis = rule`, and only for times before their source's capture start. A backfill must never change what a past forecast saw
- Reproduce: a what-if run, then a reproduce of that run, match exactly

## Model and evaluation

| | M1 solar |
| --- | --- |
| Target | Tokyo-area solar output in MW for each of the 48 slots of D (label on curtailed slots: see Decisions) |
| Inputs | Weather at the points (GHI, direct, diffuse, cloud cover, temperature) from `previous_day2` in history and the live run at `T`; sun position and clear-sky output per slot (SQL, see [Decisions](#decisions)); clear-sky index; capacity estimate; D−2 same-slot output and the median of actual over clear-sky for the last 7 complete days; day of year |
| Model | LightGBM with quantile objectives, one model each for P10, P50 and P90; quantiles sorted if they cross |
| Baselines | Persistence: D−2 same slot. Clear-sky × recent ratio: clear-sky output for D × the 7-day median ratio |
| Benchmark | TEPCO's next-day forecast. It has an unfair advantage (published after the gate) and combines wind and solar, so it is compared with solar + wind actuals, only over the capture period |
| Backtest | Rolling origin over target days from 2024-02-17 (MSM radiation is missing before it; notebook 02), retraining on a fixed cadence, features read as of each decision time; first test month after 12 months of training |
| Metrics | Daylight slots only (every slot that isn't night, as defined under Decisions, Clear-sky output): MAE normalised by capacity, pinball loss, P10–P90 coverage (target 80%), bias, skill = 1 − MAE(model) / MAE(persistence); price-weighted solar error using the JEPX Tokyo price |
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
| Weather points and weights | Done: the nine points above, each weighted by its prefecture's share of solar capacity as of `T` ([notebook 03](../../notebooks/03_solar_capacity.ipynb), section 8) | 1 |
| GCP project IDs | Done: `jepx-tokyo-forecast-dev`, `jepx-tokyo-forecast-prod`, on a dedicated billing account | 1 |
| Code license | Apache-2.0 or MIT | 9 |
| Weather models | Done: MSM for solar; ECMWF IFS 0.25° as the radiation fallback (its radiation history starts on 5–7 March 2024), because GSM has no radiation through Open-Meteo; GSM kept for cloud cover and temperature | 2 |
| ODS contract | Done: parsing changes the format, never the meaning; lineage on every row; versions, `known_at`, units and slot times in DWD ([notebook 02](../../notebooks/02_raw_to_ods.ipynb)) | 3 |
| Re-parsing | Done: append under a new `parser_version`, which is the ODS asset's code version | 3 |
| Open-Meteo ODS shape | Done: semi-long, with model and lead as columns and a column per variable | 5 |
| Every captured file as a table | Done: an ODS table for every captured file (18 with the capacity source's three), the yearly actuals and the demand outlook included, for feature exploration | 5 |
| Demand outlook history | Done: TEPCO's monthly archive (`YYYYMM_power_usage.zip`, April 2022 on) as the source's `initial` items, the last two months in its daily capture, and a monthly re-capture ([notebook 02](../../notebooks/02_raw_to_ods.ipynb), section 12.2) | 5 |
| Training window | Done: target days from 2024-02-17; later weather holes fall back to ECMWF at the same lead | 7 |
| Order of work | Done: facts and `as_of` first, then a walking skeleton end to end, then features graduating one at a time; Dagster in prod last ([Order of work](#order-of-work)) | 5 |
| VM size | `e2-medium`; resize if runs need more memory | 4 |
| Dagster UI access | IAP tunnel; Tailscale is the alternative if the tunnel gets tedious | 4 |
| Solar capacity estimate | Done: METI's FIT/FIP disclosure, tables A and B, captured daily with its summary page; `known_at` from the latest announcement date any stored copy of the page showed; extended to D by the trend through the newest four known releases ([notebook 03](../../notebooks/03_solar_capacity.ipynb)) | 5 |
| Capacity `known_at` in SQL | Done: probed in [notebook 04](../../notebooks/04_ods_to_dwd.ipynb), section 6. One query over the notices and the manifest reproduces notebook 03 for all 33 releases. The history rule reads only copies of the page taken before capture start, since a later copy moved a release's date 151 days after the fact. The margin for a file never announced is fixed at 16 days, not derived from the data, so a later release can't move the `known_at` of earlier ones; it dates end of June 2017 to 23 February 2024, so the trend takes the newest releases by the quarter they describe, never by `known_at`. Three models: `dwd_capacity_releases` (dates, 0.3 MB), `dwd_capacity_area_files` (each file's figures per part of the area: incremental, 0 MB on a day with nothing new, about 120 MB for a new release, 2 GB in full; section 6.8) and `dwd_capacity_area` (their versions, rebuilt in full). Figures are summed as `NUMERIC`, since a float sum depends on the order of addition and an incremental build must equal a full one. A release re-uploaded in place keeps its link, so the daily capture sends a `HEAD` per held release file and fetches it again when `Last-Modified` changed; the server ignores `If-Modified-Since` (section 6.6) | 5 |
| Clear-sky output | Done: capacity × the capacity-weighted clear-sky radiation at the nine points ÷ 1000 W/m², in MW: the fleet as flat panels at nameplate. Radiation from Haurwitz's formula on the sun's geometric position, both in SQL, as the mean of six samples at the 5-minute middles of each slot, added in a fixed order: TEPCO's values are slot averages, and one sample at the middle reads 22% low at dawn and dusk. Checked against SPA through a seed of reference values and a data test with a tolerance. A slot is night when the sun is below 0° at both its edges at all nine points: TEPCO shows output in none of those since February 2024. Per point in DWD; weighted in DWS, since the weights are known as of `T`. A constant error in scale cancels in the ratio baseline and doesn't matter to tree models; the likely error in shape, panel tilt, is for notebook 06 to measure ([notebook 05](../../notebooks/05_clear_sky.ipynb) checked the rest) | 5 |
| Weather DWD | Done: in step 5, after the walking skeleton runs, so the chain runs end to end sooner and weather is built on the completed initial load. `as_of` is designed with weather's selection in mind (lead 2; the older single run). Around capture start no single source spans the seam, so DWS picks a source per target day: lead 2, then a Single Run that covers D's daylight (after missing runs, MSM's newest can end before D begins), then the live capture; a data test checks that every target day has weather at `T`. Previous Runs is versioned like every source: fetched twice, only lead 0 of the hours just before the first fetch changed ([notebook 04](../../notebooks/04_ods_to_dwd.ipynb), sections 3.2 and 3.4) | 5 |
| Warehouse rules | Done: reviewed against the evidence ([notebook 02](../../notebooks/02_raw_to_ods.ipynb), sections 4 and 9; [notebook 04](../../notebooks/04_ods_to_dwd.ipynb), sections 1 to 5; [notebook 05](../../notebooks/05_clear_sky.ipynb), section 2.2). Two rules carry the rest: `known_at` right for each source, and every input read through `as_of` ([Point-in-time rules](#point-in-time-rules)). DWS never reads ODS, so the decision-time spine reads capture times from `dwd_source_captures`, which also gives each versioned model its capture start in one place; DWD models with no `known_at` are tagged timeless; labels live outside DWS. An overwritten ODS row is caught by a fingerprint per file, not by a SQL rule. Two spikes come before the rules are built: finding the `as_of` predicate per table with sqlglot, and the fingerprint | 5 |
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

# Phase 1: MVP

**Goal:** a Tokyo-area solar forecast built only from what was known at decision time, a time machine that proves it, and a measured account of what a lookahead leak does to a backtest. Two things run in prod from early on: the capture job, because some source data cannot be fetched later, and Dagster, so every capture and every pipeline run is visible from the start. Feature and model work happens in `dagster dev` on the laptop against the dev project.

This is the working plan for phase 1 and is meant to be complete on its own. The [PRD](../prd.md) holds the overall design and the later phases.

## Done when

1. The capture job runs daily in prod on schedule, and every missed capture raises an alert
2. Dagster runs locally and in prod. The prod UI shows every capture group's files per day with freshness checks, and the parse and dbt chain runs on its own when new captures land
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
| 1 | **Capture, this week** | Repo skeleton (uv, `jtf` CLI, Dockerfile, Makefile, `ci.yml`); `deploy/bootstrap` (APIs, state bucket, WIF, CI service accounts, budget, importing the hand-made budget) behind `make bootstrap`; every account-specific value as a Terraform variable, with `terraform.tfvars.example` files; `storage` module; `jtf fetch` for every capture group; Terraform for the prod capture slice; `release.yml` that builds the image and points the job at its digest; healthchecks.io checks | Three scheduled runs land files and sidecars in the prod raw bucket; a skipped run alerts |
| 2 | Initial history load | `jtf fetch --initial` per source, as one-off executions of the capture job, tagged `initial` | Raw files for every range in [Initial load](#initial-load) |
| 3 | **Dagster, locally** | `jtf/src/jtf/orchestration`: raw external assets with the bucket sensor, freshness checks per capture group, partition definitions, the first parse asset (TEPCO actuals) into dev ODS, automation on its downstream; `dagster dev` reading the prod raw bucket read-only | The local UI shows each source's captures per day with freshness checks, and a new capture starts the TEPCO parse on its own |
| 4 | **Dagster, in prod** | Terraform `platform` module: `e2-medium` VM without a public IP, Cloud NAT, IAP firewall rule, Cloud SQL `db-f1-micro` (Enterprise, Tokyo); `deploy/dagster`: `compose.prod.yaml` (webserver, daemon, code location by digest, Cloud SQL Auth Proxy) and `dagster.prod.yaml` (Postgres storage, Docker run launcher, pool limits); `release.yml` also points the code location at the new digest | Through an IAP tunnel, the prod UI shows every capture and the initial load with freshness checks green, and new captures are parsed into prod ODS on their own. A stopped capture shows up both as a failed freshness check and as a healthchecks.io alert |
| 5 | Parse and warehouse | Every parser as an ODS asset; dbt DWD with versions, MW and JST slots; the `as_of` macro; DWS solar features partitioned by decision date; SQL rules in CI. Each piece is built in `dagster dev`, then released, and runs in prod from then on | dbt tests pass; one known day checked by hand against the source files; the prod lineage graph runs from raw files to DWS |
| 6 | Time machine | Replay job (`mode`, `T` range, `fault` in run config) and `jtf replay`; run records in ADS; determinism and future-poison tests | Both tests green in CI |
| 7 | Model | Baselines, the TEPCO benchmark, M1 quantile models, rolling backtest, metrics by segment, MLflow logging; backtest and train jobs launched in `dagster dev` | Skill above 0 vs persistence on daylight slots |
| 8 | Fault scenarios | F2 and F1 run-config presets, replays and write-ups | Write-ups follow the template, with the expected outcome written first |
| 9 | README | Root README (quick start, contributing); design and results with fidelity; status; a LICENSE | Someone new can follow it |

## Data

| Source | Files | Format and units | Phase-1 use |
| --- | --- | --- | --- |
| TEPCO area actuals | `https://www.tepco.co.jp/forecast/html/images/eria_jukyu_YYYYMM_03.csv` (one per month, from 2024-02); older `area-YYYY.csv` (one per fiscal year, FY2016–2023, hourly) | Shift_JIS; row 1 `単位[MW平均]`; 30-min slots labelled by start time; **MW averaged over the slot**. Columns include `太陽光発電実績`, `太陽光出力制御量` (curtailment), `風力発電実績` | Target, lags |
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

The capture job runs outside Dagster, on Cloud Scheduler in the `Asia/Tokyo` time zone: three jobs, each starting the `jtf-fetch` Cloud Run Job with a capture group. It is the only part of the system that contacts the sources.

| Cron (JST) | Group | Fetches |
| --- | --- | --- |
| `45 8 * * *` | `morning` | Current month's `eria_jukyu` (plus the previous month on days 1–5); `AREA_YOSOKU`, `AREA_BGKEI`, `AREA_JISEKI`, `juyo-d1-j.csv`; Open-Meteo live for MSM and GSM at all points |
| `0 11 * * *` | `jepx` | JEPX spot summary for the current fiscal year |
| `0 19,22 * * *` | `evening` | `AREA_ONCE_YOSOKU`, `AREA_ONCE_BGKEI`. The file is cleared at about 23:13 and never archived |

The morning capture runs at 08:45, before `T` (09:00), so its `known_at` is at or before `T` and the forecast can use it. Each capture group pings its own healthchecks.io check on success; a missing ping alerts.

### Initial load

One-off executions of the capture job with `--initial`; Dagster then parses them like any capture.

| Source | Range | Notes |
| --- | --- | --- |
| TEPCO actuals | 2024-02 to now (30-min); FY2016–2023 hourly kept raw only | Final values: later corrections are baked in |
| JEPX | FY2016 to now | About 1.4 MB per fiscal year |
| Open-Meteo Previous Runs | 2024-01-01 to now, `previous_day2` | Matches the 30-min TEPCO labels; JMA goes back to 2018 if needed later |
| Open-Meteo Single Runs | 2026-04-02 to now: per day, the MSM 12 and 18 UTC runs and the GSM 12 UTC run | The runs that could be the latest available at `T`. Confirm each run's horizon from the data |
| TEPCO next-day forecast | None exists | The benchmark starts at the first capture |

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

**Fetch** (`jtf fetch --group G` or `--source S [--initial]`, in the capture job only) downloads a file and writes it unchanged through `storage`:

- Path: `raw/{source}/{extraction_date_jst}/{fetched_at}_{filename}`, plus `{…}.meta.json` with fetch time, URL, request parameters, response headers and any update time found inside the file (`source_time`)
- Create-only: `if_generation_match=0` in code, and the job's service account has only `roles/storage.objectCreator`, which cannot delete or overwrite. An existing file cannot be replaced even by a bug
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

**Checks:** freshness per capture group (morning by 09:00, `jepx` by 11:30, evening by 22:30); completeness (48 slots per day for actuals); units and ranges; dbt tests as asset checks. Every asset carries a code version, so a release that changes one marks its partitions out of date.

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
| F1 late weather | Drop the live run at `T` (and, in replay, the latest qualifying run) | The freshness check on the morning capture | Fallback ladder level 1, forecast flagged degraded, ranges widened | Accuracy and coverage cost of level 1 and level 2 over the backtest period |

Each write-up uses the [template](../scenarios/README.md), with the expected outcome written before the run.

## Infrastructure and CI

| Where | What | Terraform |
| --- | --- | --- |
| Prod project, Tokyo: capture slice (step 1) | Raw bucket (uniform access, soft delete on); Artifact Registry repository; service account `jtf-fetch` with `roles/storage.objectCreator` on the raw bucket only; Cloud Run Job `jtf-fetch` pinned to an image digest; three Cloud Scheduler jobs; Secret Manager entries for the healthchecks.io ping URLs | `envs/prod`, `forecast_env` module |
| Prod project, Tokyo: Dagster (step 4) | `e2-medium` VM with no public IP and a 30 GB disk; Cloud NAT; firewall rule allowing only IAP's range; Cloud SQL Postgres `db-f1-micro`, Enterprise edition, no HA, database `dagster`; service account for the VM with BigQuery and read access to the raw bucket | `envs/prod`, `platform` module |
| Dev project, Tokyo | Dev raw bucket for fetch tests; BigQuery datasets `ods`, `dwd`, `dws`, `ads` with per-developer and per-PR prefixes; read access to the prod raw bucket | `envs/dev` |
| Both, via `deploy/bootstrap` | APIs; Terraform state bucket; Workload Identity Federation for GitHub (PRs reach dev; only `main` with the `prod` environment's approval reaches prod); CI service accounts; the budget on the dedicated billing account (¥12,000 a month, alerts at 75%, 100% and on forecast) | `bootstrap` |

**Projects:** `jepx-tokyo-forecast-dev` and `jepx-tokyo-forecast-prod`, standalone (no organization), labelled `app=jtf` and `env=dev|prod`, on a dedicated billing account. The billing account ID stays out of the repo: Terraform reads it from a gitignored `terraform.tfvars`.

**Cost in phase 1:** about $1 a month until step 4; then about $45–50, mostly the VM ($30–35) and Cloud SQL ($10–12). Check the VM price in Google's calculator, since third-party figures disagree.

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
| Weather models | MSM for solar, GSM as a fallback and for coverage | 2 |
| VM size | `e2-medium`; resize if runs need more memory | 4 |
| Dagster UI access | IAP tunnel; Tailscale is the alternative if the tunnel gets tedious | 4 |
| Solar capacity estimate | Candidate: METI's FIT/FIP disclosure of installed capacity by prefecture (to verify) | 5 |
| Label on curtailed slots | Delivered + curtailed (what the weather allowed), keeping delivered as a second column; confirm after looking at the data | 7 |
| Backtest retraining cadence | Monthly | 7 |

## Risks

| Risk | Mitigation |
| --- | --- |
| A missed capture day loses that day's TEPCO next-day forecast for good | Capture runs outside Dagster on its own schedules, with its own watchdog checks; gaps are recorded as gaps, never filled with later data |
| Setting up prod Dagster takes longer than planned | Step 3 gives the full value for feature work locally; prod Dagster can slip without touching capture, which runs on its own |
| `db-f1-micro` is too small for Dagster (25 connections, 0.6 GB) | Pool sizes capped in `dagster.prod.yaml`; move to `db-g1-small` (about $26 a month) if it struggles |
| A source changes its format or encoding | Raw files kept as received; schema and range checks at parse; fixtures from real files in `jtf/tests/fixtures` |
| The initial load trips a rate limit | Throttled, resumable, one source at a time |
| The TEPCO benchmark has only a few weeks of history by the end of phase 1 | Stated as such in the results; the comparison grows in later phases |
| Phase 1 grows | Anything not in the scope list waits for its phase |

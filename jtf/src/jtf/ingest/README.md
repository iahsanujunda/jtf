# ingest

Fetch and parse, one module per source.

| Module (planned) | Source | Files |
| --- | --- | --- |
| `jepx` | JEPX spot summary | `spot_summary_{FY}.csv` via form POST |
| `tepco_actuals` | TEPCO area actuals | `eria_jukyu_YYYYMM_03.csv`; older hourly `area-YYYY.csv` |
| `tepco_forecast` | TEPCO next-day forecast | `AREA_ONCE_YOSOKU.csv`, `AREA_ONCE_BGKEI.csv` |
| `tepco_sameday` | TEPCO same-day files | `AREA_YOSOKU`, `AREA_BGKEI`, `AREA_JISEKI`, `juyo-d1-j.csv` |
| `openmeteo` | Previous Runs, Single Runs, live forecast | JSON per Kanto point and model |

**Fetch** runs only in the capture job, outside Dagster. It downloads a file and stores it unchanged through `storage/`, with a metadata sidecar: fetch time (becomes `known_at`), URL, response headers, and any update time found inside the file (`source_time`). It never parses.

**Parse** runs as Dagster ODS assets (or `jtf parse`) and reads stored raw files and emits ODS rows with `valid_time`, `source_time`, `ingested_at`, `known_at`, `load_type`, `batch_id`, `source_file` and a parser version. `known_at` comes from the raw file, never from parse time. Values stay in the source's own units and slot labels; DWD converts them.

**Legacy raw prefixes.** On 2026-11-15, before capture moved to one job per source, test runs wrote a few files under `tepco_sameday/` (now `tepco_area_forecast`, `tepco_area_bg_plan`, `tepco_area_actuals_today`, `tepco_demand_outlook`), `tepco_forecast_next_day/` (now `tepco_area_forecast_next_day`, `tepco_area_bg_plan_next_day`) and `openmeteo_live/…_forecast_jma.json` (now `forecast.json`). Raw files are never deleted, so parsers map these by filename.

Be polite to the sources: about 10 requests a day in normal running, no polling loops. TEPCO and JEPX files are Shift_JIS.

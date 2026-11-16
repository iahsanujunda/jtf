# parse

Raw files to ODS rows, one parser per file layout. The design, and a run over every raw file, is in [notebook 02](../../../../notebooks/02_raw_to_ods.ipynb).

**Parsing changes the format, never the meaning.** One ODS row per data row, ASCII column names, typed values, the source's own units and time labels. Every row carries its lineage (`source_file`, `extraction_date`, `fetched_at`, `load_type`, `source_time`, `parser_version`, then `ingested_at` and `batch_id` from the load). Versions, `known_at`, unit conversion and slot times are DWD's job, because they depend on other captures of the same file.

| Module | Holds |
| --- | --- |
| `base.py` | `RawFile` (a raw file and its sidecar), `Refused`, the lineage columns, `Parser`, `conform` |
| `tepco_actuals.py` | `eria_jukyu_YYYYMM_03.csv` → `ods.tepco_actuals` |
| `tepco_actuals_yearly.py` | `area-YYYY.csv`, FY2016–FY2023, under the same prefix → `ods.tepco_actuals_yearly` |
| `tepco_area.py` | The five `AREA_*.csv` sources, one layout read by position → `ods.tepco_area_*`, one table each |
| `tepco_demand_outlook.py` | `juyo-d1-j.csv` and its monthly archive zips → `ods.tepco_demand_outlook_peaks`, `_hourly`, `_5min`, one table per block shape |
| `jepx.py` | `spot_summary_{FY}.csv` → `ods.jepx_spot` |
| `openmeteo.py` | Previous Runs, Single Runs and live JSON → `ods.openmeteo_*`, semi-long |
| `meti_fit.py` | FIT/FIP tables A and B (`.xlsx`, six sheets) → `ods.meti_fit_capacity_pref`, `_city`, semi-long, with `ref_date` from the file name; every copy of the summary page, live or archived → `ods.meti_fit_notices` |
| `load.py` | `parse_day`: one source's files for one extraction day, once per parser version; the `raw_files` manifest |
| `bigquery.py` | The ODS dataset: append-only loads, and the queries that say what is already loaded |

Every layout has its parser: 14 tables plus the manifest, registered in `PARSERS` by table. A parser declares which files under its source's prefix it `reads`, so a source with several layouts has one parser, and one table, per layout. A parser is proven with `jtf parse` first and becomes a Dagster asset only once its table is listed in `ORCHESTRATED` ([orchestration/ods.py](../orchestration/ods.py)).

**Types:** a column is `INTEGER` where every file so far holds whole numbers (all TEPCO values, JEPX volumes), and the parser refuses a decimal there as a likely unit or layout change; JEPX prices and Open-Meteo values are `FLOAT`; labels and dates stay `STRING` as published.

**Refusing.** A parser raises `Refused` when a file's shape changed: an unknown column, a different unit row, a value that breaks a type. Nothing from that file is loaded, the manifest records it as `failed` with the reason, and the Dagster partition fails by name. A column the source drops becomes nulls.

**Once per version.** `parse_day` skips files the manifest already lists for the parser's version, so running a day twice adds nothing. Changing what a parser writes bumps its `VERSION`, which is also the Dagster asset's code version: every partition shows as out of date, and the backfill appends rows under the new version. ODS is never updated in place.

**Tables** are created by their first load: partitioned by `extraction_date`, clustered by `source_file` (`source` for the manifest), with the schema declared next to the parser.

Run outside Dagster with `jtf parse --source S --from 2026-11-15 [--to …] [--table T]`, using `JTF_RAW_URL` and `JTF_BIGQUERY_PROJECT`. Without `--table`, every layout of the source is parsed.

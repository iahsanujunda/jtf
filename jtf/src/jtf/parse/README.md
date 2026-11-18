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

Every layout has its parser: 17 tables plus the manifest, registered in `PARSERS` by table. A parser declares which files under its source's prefix it `reads`, so a source with several layouts has one parser, and one table, per layout. A parser is proven with `jtf parse` first and becomes a Dagster asset only once its table is listed in `ORCHESTRATED` ([orchestration/ods.py](../orchestration/ods.py)).

**Types:** a column is `INTEGER` where every file so far holds whole numbers (all TEPCO values, JEPX volumes), and the parser refuses a decimal there as a likely unit or layout change; JEPX prices and Open-Meteo values are `FLOAT`; labels and dates stay `STRING` as published.

**Refusing.** A parser raises `Refused` when a file's shape changed: an unknown column, a different unit row, a value that breaks a type. Nothing from that file is loaded, the manifest records it as `failed` with the reason, and the Dagster partition fails by name. A column the source drops becomes nulls.

**Once per version.** `parse_day` skips files the manifest already lists for the parser's version, so running a day twice adds nothing. Changing what a parser writes bumps its `VERSION`, which is also the Dagster asset's code version: every partition shows as out of date, and the backfill appends rows under the new version. ODS is never updated in place.

**Fingerprints** ([fingerprint.py](fingerprint.py); [notebook 07](../../../../notebooks/07_ods_fingerprints.ipynb)). ODS is append-only, but nothing in BigQuery enforces it, so a changed row can only be noticed. After a parse appends its rows, BigQuery fingerprints each loaded file, as the sum of `FARM_FINGERPRINT` (as `BIGNUMERIC`) over each row as JSON without its nulls. The loader appends one row per file to `fingerprints`, beside the manifest, with BigQuery's own time; then it writes the manifest. A load stopped before its manifest gets its fingerprints when it gets its manifest row, unless they were already recorded. Files loaded before fingerprints existed were recorded once by `jtf ods backfill-fingerprints`, as `backfill`: they are guarded from then on. Each ODS asset checks its table after every load (`rows_as_loaded`). The check reads only partitions whose row count differs from the recorded ones, or whose last change came after their last record, so normally none, at about 15 MB billed for the metadata. `jtf ods verify` runs the same check; `--all` reads every file again (all of ODS, about 4 GB).

**Tables** are created by their first load: partitioned by `extraction_date`, clustered by `source_file` (`source` for the manifest), with the schema declared next to the parser.

Run outside Dagster with `jtf parse --source S --from 2026-11-15 [--to …] [--table T]`, using `JTF_RAW_URL` and `JTF_BIGQUERY_PROJECT`. Without `--table`, every layout of the source is parsed.

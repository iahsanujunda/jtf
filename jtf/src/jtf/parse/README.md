# parse

Raw files to ODS rows, one parser per file layout. The design, and a run over every raw file, is in [notebook 02](../../../../notebooks/02_raw_to_ods.ipynb).

**Parsing changes the format, never the meaning.** One ODS row per data row, ASCII column names, typed values, the source's own units and time labels. Every row carries its lineage (`source_file`, `extraction_date`, `fetched_at`, `load_type`, `source_time`, `parser_version`, then `ingested_at` and `batch_id` from the load). Versions, `known_at`, unit conversion and slot times are DWD's job, because they depend on other captures of the same file.

| Module | Holds |
| --- | --- |
| `base.py` | `RawFile` (a raw file and its sidecar), `Refused`, the lineage columns, `Parser`, `conform` |
| `tepco_actuals.py` | `eria_jukyu_YYYYMM_03.csv` → `ods.tepco_actuals`. The yearly `area-YYYY.csv` files under the same prefix get a parser and table of their own |
| `load.py` | `parse_day`: one source's files for one extraction day, once per parser version; the `raw_files` manifest |
| `bigquery.py` | The ODS dataset: append-only loads, and the queries that say what is already loaded |

Step 5 adds the other layouts, each registered in `PARSERS` by its table. A parser declares which files under its source's prefix it `reads`, so a source with several layouts has one parser, and one table, per layout.

**Refusing.** A parser raises `Refused` when a file's shape changed: an unknown column, a different unit row, a value that breaks a type. Nothing from that file is loaded, the manifest records it as `failed` with the reason, and the Dagster partition fails by name. A column the source drops becomes nulls.

**Once per version.** `parse_day` skips files the manifest already lists for the parser's version, so running a day twice adds nothing. Changing what a parser writes bumps its `VERSION`, which is also the Dagster asset's code version: every partition shows as out of date, and the backfill appends rows under the new version. ODS is never updated in place.

**Tables** are created by their first load: partitioned by `extraction_date`, clustered by `source_file` (`source` for the manifest), with the schema declared next to the parser.

Run outside Dagster with `jtf parse --source tepco_actuals --from 2026-11-15 [--to …]`, using `JTF_RAW_URL` and `JTF_BIGQUERY_PROJECT`.

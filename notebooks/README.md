# notebooks

Exploration only. No feature logic lives here: a feature worth keeping moves into a dbt model, so there is no pandas-to-SQL rewrite and no train/serve skew.

| Notebook | Reads | Purpose |
| --- | --- | --- |
| [00_source_probe](00_source_probe.ipynb) | The live sources, cached per day in `data/probe/` | How each source was probed, and how every finding became an extraction rule |
| [01_history_probe](01_history_probe.ipynb) | Historical files and APIs, cached per day in `data/probe/` | What history exists, how it differs from today's files, what loading it costs; the design of step 2 |
| [02_raw_to_ods](02_raw_to_ods.ipynb) | The prod raw bucket (read-only), cached in `data/raw-cache/` | How raw files become ODS tables: the row contract, versions and `known_at`, one parser per layout run over every file, the Dagster mapping |
| [03_solar_capacity](03_solar_capacity.ipynb) | METI's FIT/FIP disclosure site and archived copies of it, cached in `data/probe/capacity/`; TEPCO actuals from dev BigQuery | The solar capacity source end to end: when each release became known, how old the figure is when used, the TEPCO-area cut at the Fuji River, carrying vs extrapolating the stale figure, the weather-point weights |
| [04_ods_to_dwd](04_ods_to_dwd.ipynb) | The ODS tables in dev BigQuery | From ODS to DWD in SQL, for the solar path: capture start per source, versions and `known_at` of the target (a late backfill among them), which weather a forecast at `T` can use, hourly weather onto slots, the benchmark, and `as_of` as a range join over every decision time |
| [05_clear_sky](05_clear_sky.ipynb) | dbt's `dwd_tepco_actuals` in dev BigQuery (latest versions); pvlib as the reference | Clear-sky output in SQL: the sun's position against SPA, night by the sun against TEPCO's output, one sample per slot or six, clear days in time |
| [06_warehouse_against_sources](06_warehouse_against_sources.ipynb) | dbt's DWD models in dev BigQuery, every version, and the ODS manifest | Whether what step 5 built is true to its sources: `known_at` per source against the fetches that brought each version; one known day traced by hand (next) |
| From 07 on | The dbt models in dev BigQuery, read through `as_of`; MLflow runs | Feature and model exploration on top of the pipeline: a feature that earns its place becomes a DWS model, so its logic lives only in dbt |

Run them with the notebook dependencies: `uv sync --group notebook`, then pick the `.venv` kernel in VS Code or Jupyter.

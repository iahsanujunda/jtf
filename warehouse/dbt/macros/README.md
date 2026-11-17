# macros

| Macro | Does |
| --- | --- |
| [`as_of(versions, t)`](as_of.sql) | A join condition: the version current at `t`, `known_at ≤ t < known_until`. Serves one `T` or every `T` of a backtest in one query. The only way DWS may read DWD |
| [`sun_position(times, points)`](sun_position.sql) | CTEs ending in `position`: the sun's geometric height and Haurwitz clear-sky radiation for every timestamp and point. NOAA's equations, within 0.015° of SPA ([notebook 05](../../../notebooks/05_clear_sky.ipynb)) |
| [`only_files(files_sql)`](only_files.sql) | For an incremental model over ODS: the fetch days and files that `files_sql` lists, written in as constants, so BigQuery reads only their partitions and blocks. A subquery or a join on the same list reads the whole table ([notebook 04](../../../notebooks/04_ods_to_dwd.ipynb), section 6.8). Adds nothing on a full build or in a unit test; the model filters on the list in SQL as well |
| [`weather_versions(table, key, rule)`](weather.sql) | The versions of a history-only Open-Meteo table per point, model, `key` (lead or run) and hour: each file as last parsed, a first version known by `rule`, a changed value at the fetch that held it, never before the version it replaces. Used by the Previous Runs and Single Runs models, so both version by the same rule |
| [`weather_from_ods(alias)`, `weather_variables()`](weather.sql) | Open-Meteo's six variables under DWD's names, units in the name: `ghi_w_m2`, `direct_w_m2`, `diffuse_w_m2`, `cloud_cover_pct`, `temperature_c`, `wind_speed_kmh` |
| Unit conversions (planned) | kWh per slot ÷ 500 → MW; 万kW × 10 → MW |
| Slot normalisation (planned) | Slot codes 1–48, `0000`–`2400` and start-time labels → slot start in JST |

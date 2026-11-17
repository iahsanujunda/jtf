# macros

| Macro | Does |
| --- | --- |
| [`as_of(versions, t)`](as_of.sql) | A join condition: the version current at `t`, `known_at ≤ t < known_until`. Serves one `T` or every `T` of a backtest in one query. The only way DWS may read DWD |
| [`sun_position(times, points)`](sun_position.sql) | CTEs ending in `position`: the sun's geometric height and Haurwitz clear-sky radiation for every timestamp and point. NOAA's equations, within 0.015° of SPA ([notebook 05](../../../notebooks/05_clear_sky.ipynb)) |
| [`only_files(files_sql)`](only_files.sql) | For an incremental model over ODS: the fetch days and files that `files_sql` lists, written in as constants, so BigQuery reads only their partitions and blocks. A subquery or a join on the same list reads the whole table ([notebook 04](../../../notebooks/04_ods_to_dwd.ipynb), section 6.8). Adds nothing on a full build or in a unit test; the model filters on the list in SQL as well |
| Unit conversions (planned) | kWh per slot ÷ 500 → MW; 万kW × 10 → MW |
| Slot normalisation (planned) | Slot codes 1–48, `0000`–`2400` and start-time labels → slot start in JST |

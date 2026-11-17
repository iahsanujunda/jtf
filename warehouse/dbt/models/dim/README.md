# DIM

What nothing is learned about over time, so it has no `known_at` and every layer reads it directly. It reads only itself and the seeds, which sit here too: nothing above can leak into it, and a rebuild always gives the same table.

- `dim_sun_slots`: the sun at each weather point per slot, its height at the slot's edges and clear-sky radiation as the mean of six samples, computed from time and place alone over a fixed range (2016-04 to 2030)
- `dim_daylight`: night per slot, the sun below 0° at both edges at all nine points; every other slot is daylight, the metrics' filter
- Seeds: `dim_weather_points` (the nine points, kept equal to the capture's by a pytest), `dim_tepco_area_parts` (the parts of the area in METI's capacity tables, each named by its weather point) and `dim_sun_reference` (SPA's values, which the data test `dim_sun_slots__matches_spa` compares the SQL against; `scripts/write_sun_reference.py` rewrites it)

Designed in [notebook 05](../../../../notebooks/05_clear_sky.ipynb), where they still carry their first names, `dwd_sun_slots` and `dwd_daylight`. Anything read from a captured source is not DIM, whatever it describes: it was learned at some time, so it is DWD with a `known_at`.

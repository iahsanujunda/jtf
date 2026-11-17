# DWD

Cleaned, typed detail per area and slot. Converts every power value to MW averaged over the slot and every slot label to its start time in JST. Keeps every version with its `known_at`, `known_until` and `basis`: a changed value is a new version, the same value seen again is not. Weather is kept per run, never collapsed. Range and slot checks here back F4 and F5.

`dwd_capacity_releases` has one row per quarterly capacity release, not per version: a release's date is one fact, so its `known_until` is always null and `as_of` reads it like any other model. A re-uploaded file is a new version of the release's figures, never a new date for the release ([notebook 04](../../../../notebooks/04_ods_to_dwd.ipynb), section 6).

Two models have no source: `dwd_sun_slots` (the sun at each weather point per slot: its height at the slot's edges, and clear-sky radiation as the mean of six samples) and `dwd_daylight` (night per slot: the sun below 0° at both edges at all nine points). They are computed from time and place alone, over a fixed range, so they carry no `known_at`; the seed `weather_points` holds the points, kept equal to the capture's by a pytest, and `sun_reference` holds SPA's values for a data test ([notebook 05](../../../../notebooks/05_clear_sky.ipynb); `scripts/write_sun_reference.py` rewrites it).

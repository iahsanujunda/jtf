-- The SQL's sun against NREL's SPA (seeds/dim_sun_reference.csv, written with pvlib): within 0.05°
-- and 1 W/m² at every reference point and slot, and none of them missing from the model.
{{ config(meta={'dagster': {'ref': {'name': 'dim_sun_slots'}}}) }}

select
    r.point,
    r.slot_start,
    r.elevation_start as spa_elevation_start,
    s.elevation_start,
    r.elevation_end as spa_elevation_end,
    s.elevation_end,
    r.clear_sky_ghi as spa_clear_sky_ghi,
    s.clear_sky_ghi
from {{ ref('dim_sun_reference') }} as r
left join {{ ref('dim_sun_slots') }} as s
    on s.point = r.point and s.slot_start = r.slot_start
where s.slot_start is null
    or abs(s.elevation_start - r.elevation_start) > 0.05
    or abs(s.elevation_end - r.elevation_end) > 0.05
    or abs(s.clear_sky_ghi - r.clear_sky_ghi) > 1

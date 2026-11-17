-- Night (the sun below 0° at both edges of the slot at all nine points) has no clear sky (F4).
{{ config(meta={'dagster': {'ref': {'name': 'dws_solar_features'}}}) }}

select f.decision_time, f.slot_start, f.clear_sky_mw
from {{ ref('dws_solar_features') }} as f
inner join {{ ref('dwd_daylight') }} as d using (slot_start)
where d.is_night and f.clear_sky_mw != 0

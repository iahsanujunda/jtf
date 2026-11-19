-- Every daylight slot of every decision time has the area's forecast radiation: dws_weather
-- holds weather at T for all of them, at every point, so an empty one here means a point was
-- lost on the way to the area's figure.
{{ config(meta={'dagster': {'ref': {'name': 'dws_solar_features'}}}) }}

select f.decision_time, f.slot_start
from {{ ref('dws_solar_features') }} as f
inner join {{ ref('dim_daylight') }} as d on d.slot_start = f.slot_start
where not d.is_night and f.forecast_ghi_w_m2 is null

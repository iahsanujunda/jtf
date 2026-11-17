-- Every decision time has, for every daylight slot of its target day, every variable of every
-- model at every point (radiation only for a model that has it): the step-5 check that every
-- target day has weather known at T. Strict, with no exceptions: at lead 2 every hole is held by
-- another model (notebooks/04_ods_to_dwd.ipynb, section 3.6). A missed live capture fails it for
-- the day it would have served, as it should: that day had no weather at T.
{{ config(meta={'dagster': {'ref': {'name': 'dws_weather'}}}) }}

select s.decision_time, s.slot_start, p.point, m.model
from {{ ref('dws_decision_slots') }} as s
inner join {{ ref('dim_daylight') }} as d on d.slot_start = s.slot_start and not d.is_night
cross join {{ ref('dim_weather_points') }} as p
cross join {{ ref('dim_weather_models') }} as m
left join {{ ref('dws_weather') }} as w
    on w.decision_time = s.decision_time
    and w.slot_start = s.slot_start
    and w.point = p.point
    and w.model = m.model
where w.decision_time is null
    or w.cloud_cover_pct is null
    or w.temperature_c is null
    or w.wind_speed_kmh is null
    or (m.has_radiation and (w.ghi_w_m2 is null or w.direct_w_m2 is null or w.diffuse_w_m2 is null))

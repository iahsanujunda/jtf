-- F4: solar is 0 at night, in every version. A slot is night when the sun is below the horizon at
-- both its edges at all nine points (dim_daylight); TEPCO showed output in none of those from
-- February 2024 to November 2026 (notebooks/05_clear_sky.ipynb, section 2).
{{ config(meta={'dagster': {'ref': {'name': 'dwd_tepco_actuals'}}}) }}

select a.*, d.highest_edge
from {{ ref('dwd_tepco_actuals') }} as a
inner join {{ ref('dim_daylight') }} as d on d.slot_start = a.slot_start
where a.measure = 'solar' and d.is_night and a.value_mw > 0

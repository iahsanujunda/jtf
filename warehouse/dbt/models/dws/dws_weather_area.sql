{#
  The weather forecast for the whole area: per decision time and slot of its target day, each
  variable of the solar path's model (MSM) as one figure, the nine points weighted by their share
  of solar capacity as known at T (dws_point_weights), the way clear-sky output weights its
  radiation. A day's forecast clearness, read this way, follows its real clearness at 0.82
  (notebooks/10_what_the_forecast_knows.ipynb).

  A variable is empty where any point lacks it, never a sum over the points that have it; after
  dws_weather's fill none does in daylight. Summed exactly (NUMERIC), so a rebuild gives the same
  figure. Wind speed is a weighted mean of speeds, not of vectors: a size, with no direction.
#}
{% set variables = weather_variables() %}

select
    w.decision_time,
    w.target_day,
    w.slot_start,
    {%- for v in variables %}
    if(
        count(w.{{ v }}) = count(*),
        cast(sum(cast(p.weight as numeric) * cast(w.{{ v }} as numeric)) as float64),
        null
    ) as {{ v }},
    {%- endfor %}
    count(*) as points
from {{ ref('dws_weather') }} as w
inner join {{ ref('dws_point_weights') }} as p
    on p.decision_time = w.decision_time and p.point = w.point
where w.model = 'jma_msm'
group by w.decision_time, w.target_day, w.slot_start

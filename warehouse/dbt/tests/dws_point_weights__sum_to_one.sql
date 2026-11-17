-- At every decision time, every part of the area has a weight and the weights add up to one.
{{ config(meta={'dagster': {'ref': {'name': 'dws_point_weights'}}}) }}

select decision_time, count(*) as points, sum(weight) as total
from {{ ref('dws_point_weights') }}
group by decision_time
having count(*) != (select count(distinct point) from {{ ref('tepco_area_parts') }})
    or abs(sum(weight) - 1) > 1e-6

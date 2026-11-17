-- One source serves each decision time, for every slot, point and model: features never mix a
-- live capture with a run from the history on the same target day.
{{ config(meta={'dagster': {'ref': {'name': 'dws_weather'}}}) }}

select decision_time, count(distinct source) as sources
from {{ ref('dws_weather') }}
group by decision_time
having count(distinct source) != 1

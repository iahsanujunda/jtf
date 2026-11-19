-- Every decision time has a fidelity for each of its four inputs: a missing one would leave a
-- run's fidelity unknown, not exact.
{{ config(meta={'dagster': {'ref': {'name': 'dws_fidelity'}}}) }}

with decisions as (
    select distinct decision_time from {{ ref('dws_decision_slots') }}
)

select d.decision_time, count(f.input) as inputs
from decisions as d
left join {{ ref('dws_fidelity') }} as f on f.decision_time = d.decision_time
group by d.decision_time
having count(distinct f.input) != 4

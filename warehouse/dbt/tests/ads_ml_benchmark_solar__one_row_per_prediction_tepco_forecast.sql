-- The comparison holds each prediction on a slot TEPCO forecast exactly once, and no other: the
-- joins only add columns. A doubled or lost row would score the two forecasts on different slots.
{{ config(meta={'dagster': {'ref': {'name': 'ads_ml_benchmark_solar'}}}) }}

with expected as (
    select p.run_id, count(*) as predicted
    from {{ source('ads', 'ads_ml_prediction_solar') }} as p
    inner join {{ ref('dwd_tepco_forecast_next_day') }} as t
        on t.slot_start = p.slot_start and t.measure = 'wind_solar' and t.known_until is null
    group by p.run_id
),

held as (
    select run_id, count(*) as compared
    from {{ ref('ads_ml_benchmark_solar') }}
    group by run_id
)

select *
from expected
full join held using (run_id)
where predicted is distinct from compared

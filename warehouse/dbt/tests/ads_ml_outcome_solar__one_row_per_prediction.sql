-- Every prediction has exactly one outcome: the joins only add columns, so a run that lost or
-- doubled rows on the way would be scored on the wrong slots.
{{ config(meta={'dagster': {'ref': {'name': 'ads_ml_outcome_solar'}}}) }}

with predictions as (
    select run_id, model, count(*) as predicted
    from {{ source('ads', 'ads_ml_prediction_solar') }}
    group by run_id, model
),

outcomes as (
    select run_id, model, count(*) as scored
    from {{ ref('ads_ml_outcome_solar') }}
    group by run_id, model
)

select *
from predictions
full join outcomes using (run_id, model)
where predicted is distinct from scored

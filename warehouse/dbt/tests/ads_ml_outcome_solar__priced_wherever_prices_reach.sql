-- Every outcome on a day the price table covers has its price: a slot without one would drop out of
-- the price-weighted error without a word. Outside the table's days (a day not yet traded, or a
-- warehouse that holds no prices at all) there is nothing to hold an outcome to.
{{ config(meta={'dagster': {'ref': {'name': 'ads_ml_outcome_solar'}}}) }}

with held as (
    select min(slot_start) as first_slot, max(slot_start) as last_slot
    from {{ ref('dwd_jepx_spot') }}
    where measure = 'price_tokyo'
)

select o.run_id, o.slot_start
from {{ ref('ads_ml_outcome_solar') }} as o
cross join held
where o.slot_start between held.first_slot and held.last_slot and o.price_yen_kwh is null

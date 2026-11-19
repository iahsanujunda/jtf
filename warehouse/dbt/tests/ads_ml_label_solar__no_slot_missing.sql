-- F6: every half hour between the first label and the last has one, with a value. A slot dropped
-- from TEPCO's file, a whole day, or a blank would leave training and scoring with a hole nobody
-- sees. None from February 2024 to November 2026 (notebooks/08_solar_label.ipynb, section 5).
-- The newest day is partial by nature, so the span ends at the last slot published.
{{ config(meta={'dagster': {'ref': {'name': 'ads_ml_label_solar'}}}) }}

with span as (
    select min(slot_start) as first_slot, max(slot_start) as last_slot
    from {{ ref('ads_ml_label_solar') }}
)

select d.slot_start, l.label_mw, l.delivered_mw, l.curtailed_mw
from {{ ref('dim_daylight') }} as d
cross join span
left join {{ ref('ads_ml_label_solar') }} as l on l.slot_start = d.slot_start
where d.slot_start between span.first_slot and span.last_slot
    and (l.slot_start is null or l.label_mw is null)

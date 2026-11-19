-- F6: no lit slot without output. A slot is lit when clear sky allows at least 5% of capacity, as
-- estimated at its decision time. Even the dullest day since February 2024 gave 4.8% of clear sky
-- over its lit slots, and no lit slot was at 0 (notebooks/08_solar_label.ipynb, section 5): a 0
-- there is a missing value written as 0. The label is read, not delivered output: a slot
-- curtailed in full would deliver 0 with nothing wrong.
{{ config(meta={'dagster': {'ref': {'name': 'ads_ml_label_solar'}}}) }}

select l.slot_start, l.label_mw, l.delivered_mw, l.curtailed_mw, f.clear_sky_mw, f.capacity_mw
from {{ ref('ads_ml_label_solar') }} as l
inner join {{ ref('dws_solar_features') }} as f on f.slot_start = l.slot_start
where f.clear_sky_mw >= 0.05 * f.capacity_mw and l.label_mw <= 0

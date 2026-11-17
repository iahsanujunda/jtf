-- F5, the ceiling: no slot's solar, curtailment added back, above installed capacity. The highest
-- seen is 0.87 of it (2024-02-17 to 2026-11-17); above it is a unit error, such as kWh per slot read
-- as MW, which is 500 times too large.
{{ config(meta={'dagster': {'ref': {'name': 'dwd_tepco_actuals'}}}) }}

with slots as (
    select slot_start, sum(ifnull(value_mw, 0)) as mw
    from {{ ref('dwd_tepco_actuals') }}
    where known_until is null and measure in ('solar', 'solar_curtailed')
    group by slot_start
)

select s.slot_start, s.mw, c.capacity_mw
from slots as s
inner join {{ ref('dws_capacity') }} as c
    on c.target_day = date(s.slot_start, 'Asia/Tokyo')
where s.mw > c.capacity_mw

-- F5, the floor: every complete week's peak, curtailment added back, above a fifth of installed
-- capacity. 万kW read as MW is ten times too small and passes the ceiling; it would put a week's
-- peak below about 0.09. The lowest real week was 0.345 (week of 2026-09-07, of 143 complete
-- weeks to 2026-11-17), too close to the third that notebook 03 suggested.
{{ config(meta={'dagster': {'ref': {'name': 'dwd_tepco_actuals'}}}) }}

with slots as (
    select slot_start, sum(ifnull(value_mw, 0)) as mw
    from {{ ref('dwd_tepco_actuals') }}
    where known_until is null and measure in ('solar', 'solar_curtailed')
    group by slot_start
),

weeks as (
    select
        date_trunc(c.target_day, isoweek) as week,
        max(s.mw / c.capacity_mw) as peak_over_capacity,
        count(distinct c.target_day) as days
    from slots as s
    inner join {{ ref('dws_capacity') }} as c
        on c.target_day = date(s.slot_start, 'Asia/Tokyo')
    group by week
)

select *
from weeks
where days = 7 and peak_over_capacity < 0.2

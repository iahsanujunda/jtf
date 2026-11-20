-- JEPX trades every half hour of every day: from the first delivery day held to the last, each
-- day has 48 slots of each price. A missing day or slot would silently drop its forecasts' misses
-- from a price-weighted error.
with days as (
    select day
    from (
        select
            min(date(slot_start, 'Asia/Tokyo')) as first_day,
            max(date(slot_start, 'Asia/Tokyo')) as last_day
        from {{ ref('dwd_jepx_spot') }}
    ), unnest(generate_date_array(first_day, last_day)) as day
),

held as (
    select date(slot_start, 'Asia/Tokyo') as day, measure, count(*) as slots
    from {{ ref('dwd_jepx_spot') }}
    where known_until is null
    group by day, measure
)

select d.day, m as measure, coalesce(h.slots, 0) as slots
from days as d
cross join unnest(['price_tokyo', 'price_system']) as m
left join held as h on h.day = d.day and h.measure = m
where coalesce(h.slots, 0) != 48

{#
  Every decision time T with the 48 slots of its target day D: T is 09:00 JST on D−1. The range
  runs from the first target day to the day after the newest daily capture: nothing here reads the
  clock. That read of dwd_source_captures is the as_of rule's one named exemption (rules/README.md):
  it decides which decision times exist and feeds no value, so later captures only add rows at
  the end and never change one. Built from captures known at each T instead, a day with a
  capture outage would vanish from the axis rather than show up as missing weather.
#}
with bounds as (
    select
        date('{{ var("first_target_day") }}') as first_day,
        date_add(date(max(known_at), 'Asia/Tokyo'), interval 1 day) as last_day
    from {{ ref('dwd_source_captures') }}
),

days as (
    select target_day
    from bounds, unnest(generate_date_array(first_day, last_day)) as target_day
)

select
    timestamp(
        datetime(date_sub(target_day, interval 1 day), time({{ var('decision_hour_jst') }}, 0, 0)),
        'Asia/Tokyo'
    ) as decision_time,
    target_day,
    slot + 1 as slot_code,
    timestamp_add(
        timestamp(datetime(target_day, time(0, 0, 0)), 'Asia/Tokyo'), interval 30 * slot minute
    ) as slot_start
from days, unnest(generate_array(0, 47)) as slot

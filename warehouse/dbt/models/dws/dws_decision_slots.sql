{#
  Every decision time T with the 48 slots of its target day D: T is 09:00 JST on D−1. The range
  runs from the first target day to the day after the newest daily capture, read from the
  manifest: nothing here reads the clock.
#}
with bounds as (
    select
        date('{{ var("first_target_day") }}') as first_day,
        date_add(date(max(fetched_at), 'Asia/Tokyo'), interval 1 day) as last_day
    from {{ source('ods', 'raw_files') }}
    where load_type = 'daily'
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

-- TEPCO publishes its forecast for a day after that day's decision time (09:00 JST the day
-- before): no version may be known by then. If one were, `as_of` would hand the yardstick to a
-- feature, and a forecast could be made from the forecast it is measured against.
select *
from {{ ref('dwd_tepco_forecast_next_day') }}
where known_at <= timestamp(
    datetime(
        date_sub(date(slot_start, 'Asia/Tokyo'), interval 1 day),
        time({{ var('decision_hour_jst') }}, 0, 0)
    ),
    'Asia/Tokyo'
)

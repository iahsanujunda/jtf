-- A day TEPCO forecast has all 48 slots of wind and solar, as the file is laid out. A day with
-- fewer was captured half-written or half-emptied, and a score over it would be over other slots
-- than its neighbours'. A day with no capture has no rows and is not a failure: it is a gap.
select date(slot_start, 'Asia/Tokyo') as day, count(*) as slots
from {{ ref('dwd_tepco_forecast_next_day') }}
where measure = 'wind_solar' and known_until is null
group by day
having count(*) != 48

-- Bidding for a delivery day closes at 10:00 JST on the day before (JEPX trading guide, p.8), so
-- no price is known earlier, by the rule or by a capture. One known earlier would let a forecast
-- made at 09:00 read the price of the day it forecasts.
select *
from {{ ref('dwd_jepx_spot') }}
where known_at < timestamp(
    datetime(date_sub(date(slot_start, 'Asia/Tokyo'), interval 1 day), time '10:00:00'),
    'Asia/Tokyo'
)

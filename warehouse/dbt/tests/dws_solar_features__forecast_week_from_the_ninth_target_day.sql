-- The week's mean forecast needs the forecasts of the seven target days D−8 to D−2, so it is empty
-- on the first eight target days and on no other: a gap later would leave the scaled baseline
-- silently falling back to the plain mean.
with numbered as (
    select
        decision_time,
        slot_start,
        forecast_ghi_w_m2,
        forecast_ghi_mean_7d_w_m2,
        dense_rank() over (order by target_day) as nth_target_day
    from {{ ref('dws_solar_features') }}
)

select decision_time, slot_start, nth_target_day
from numbered
where forecast_ghi_w_m2 is not null
    and (nth_target_day > 8) != (forecast_ghi_mean_7d_w_m2 is not null)

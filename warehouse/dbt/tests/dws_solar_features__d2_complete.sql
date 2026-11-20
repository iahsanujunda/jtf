-- Lags use D−2 because it is complete at T in both eras (notebook 04, section 5.3): every
-- decision time must see all 48 slots of it.
-- Wind comes in the same file, so it is held to the same.
select decision_time, count(solar_d2_mw) as slots_known, count(wind_d2_mw) as wind_slots_known
from {{ ref('dws_solar_features') }}
group by decision_time
having count(solar_d2_mw) < 48 or count(wind_d2_mw) < 48

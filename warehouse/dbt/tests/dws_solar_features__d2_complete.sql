-- Lags use D−2 because it is complete at T in both eras (notebook 04, section 5.3): every
-- decision time must see all 48 slots of it.
select decision_time, count(solar_d2_mw) as slots_known
from {{ ref('dws_solar_features') }}
group by decision_time
having count(solar_d2_mw) < 48

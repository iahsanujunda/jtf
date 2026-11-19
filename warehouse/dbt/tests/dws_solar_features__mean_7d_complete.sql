-- The seven days D−8 to D−2 are complete at T in both eras, as D−2 is (notebook 04, section 5.3):
-- every decision time must have the seven-day mean in all 48 slots.
select decision_time, count(solar_mean_7d_mw) as slots_known
from {{ ref('dws_solar_features') }}
group by decision_time
having count(solar_mean_7d_mw) < 48

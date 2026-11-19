{#
  Solar features per decision time and target slot, as known at T, for every decision time in one
  pass. Reads DWD only through as_of, and other DWS models by decision time.
  - solar_d2_mw: the same slot on D−2, the newest day complete at T (notebook 04, section 5.3)
  - solar_mean_7d_mw: the mean of the same slot over D−8 to D−2, the last seven days complete at
    T, delivered and curtailed output added, each at the version current at T; null unless all
    seven days are known. The second baseline, and an input to the model
    (notebooks/09_output_against_clear_sky.ipynb, section 5). Summed exactly (NUMERIC).
  - forecast_*: the weather forecast at T for the slot, over the whole area (dws_weather_area)
  - forecast_ghi_mean_7d_w_m2: the mean of the forecast radiation for the same slot on the seven
    target days D−8 to D−2, each as its own decision time knew it, so all known at T; null unless
    all seven exist. With the seven-day mean it gives the third baseline: the mean scaled by how
    tomorrow's forecast compares with that week's
    (notebooks/10_what_the_forecast_knows.ipynb, section 4)
  - capacity_mw: installed solar on D as estimated at T (dws_capacity)
  - clear_sky_mw: the output a cloudless slot would allow, the fleet as flat panels at nameplate:
    capacity × the clear-sky radiation at the nine points weighted by their capacity at T
    ÷ 1000 W/m² (plan, Decisions, "Clear-sky output"; notebooks/05_clear_sky.ipynb). Summed
    exactly (NUMERIC), so a rebuild gives the same figure to the bit.
#}

with clear_sky as (
    select
        s.decision_time,
        s.slot_start,
        sum(cast(w.weight as numeric) * cast(sun.clear_sky_ghi as numeric)) as ghi
    from {{ ref('dws_decision_slots') }} as s
    inner join {{ ref('dws_point_weights') }} as w
        on w.decision_time = s.decision_time
    inner join {{ ref('dim_sun_slots') }} as sun
        on sun.point = w.point and sun.slot_start = s.slot_start
    -- dim_sun_slots runs from 2016; a constant bound keeps its earlier partitions unread.
    where sun.slot_start >= timestamp('{{ var("first_target_day") }}', 'Asia/Tokyo')
    group by s.decision_time, s.slot_start
),

-- The same slot on each of the seven days D−8 to D−2: two measures a day, so 14 readings
week as (
    select
        s.decision_time,
        s.slot_start,
        count(w.value_mw) as readings,
        sum(cast(w.value_mw as numeric)) as mw
    from {{ ref('dws_decision_slots') }} as s
    cross join unnest(generate_array(2, 8)) as days_back
    inner join {{ ref('dwd_tepco_actuals') }} as w
        on w.slot_start = timestamp_sub(s.slot_start, interval days_back day)
        and w.measure in ('solar', 'solar_curtailed')
        and {{ as_of('w', 's.decision_time') }}
    group by s.decision_time, s.slot_start
),

-- The forecast radiation for the same slot on each of the seven target days D−8 to D−2
forecast_week as (
    select
        s.decision_time,
        s.slot_start,
        count(a.ghi_w_m2) as days,
        sum(cast(a.ghi_w_m2 as numeric)) as ghi
    from {{ ref('dws_decision_slots') }} as s
    cross join unnest(generate_array(2, 8)) as days_back
    inner join {{ ref('dws_weather_area') }} as a
        on a.slot_start = timestamp_sub(s.slot_start, interval days_back day)
    group by s.decision_time, s.slot_start
)

select
    s.decision_time,
    s.target_day,
    s.slot_start,
    d2.value_mw as solar_d2_mw,
    if(wk.readings = 14, cast(wk.mw / 7 as float64), null) as solar_mean_7d_mw,
    c.capacity_mw,
    cast(cast(c.capacity_mw as numeric) * cs.ghi / 1000 as float64) as clear_sky_mw,
    fa.ghi_w_m2 as forecast_ghi_w_m2,
    fa.direct_w_m2 as forecast_direct_w_m2,
    fa.diffuse_w_m2 as forecast_diffuse_w_m2,
    fa.cloud_cover_pct as forecast_cloud_cover_pct,
    fa.temperature_c as forecast_temperature_c,
    if(fw.days = 7, cast(fw.ghi / 7 as float64), null) as forecast_ghi_mean_7d_w_m2
from {{ ref('dws_decision_slots') }} as s
left join {{ ref('dwd_tepco_actuals') }} as d2
    on d2.measure = 'solar'
    and d2.slot_start = timestamp_sub(s.slot_start, interval 2 day)
    and {{ as_of('d2', 's.decision_time') }}
left join week as wk
    on wk.decision_time = s.decision_time and wk.slot_start = s.slot_start
left join {{ ref('dws_capacity') }} as c
    on c.decision_time = s.decision_time
left join clear_sky as cs
    on cs.decision_time = s.decision_time and cs.slot_start = s.slot_start
left join {{ ref('dws_weather_area') }} as fa
    on fa.decision_time = s.decision_time and fa.slot_start = s.slot_start
left join forecast_week as fw
    on fw.decision_time = s.decision_time and fw.slot_start = s.slot_start

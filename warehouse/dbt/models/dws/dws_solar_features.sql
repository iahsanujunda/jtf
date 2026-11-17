{#
  Solar features per decision time and target slot, as known at T, for every decision time in one
  pass. Reads DWD only through as_of, and other DWS models by decision time.
  - solar_d2_mw: the same slot on D−2, the newest day complete at T (notebook 04, section 5.3)
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
)

select
    s.decision_time,
    s.target_day,
    s.slot_start,
    d2.value_mw as solar_d2_mw,
    c.capacity_mw,
    cast(cast(c.capacity_mw as numeric) * cs.ghi / 1000 as float64) as clear_sky_mw
from {{ ref('dws_decision_slots') }} as s
left join {{ ref('dwd_tepco_actuals') }} as d2
    on d2.measure = 'solar'
    and d2.slot_start = timestamp_sub(s.slot_start, interval 2 day)
    and {{ as_of('d2', 's.decision_time') }}
left join {{ ref('dws_capacity') }} as c
    on c.decision_time = s.decision_time
left join clear_sky as cs
    on cs.decision_time = s.decision_time and cs.slot_start = s.slot_start

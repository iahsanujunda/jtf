{#
  Solar features per decision time and target slot, as known at T, for every decision time in one
  pass. Reads DWD only through as_of. The walking skeleton's first input: the same slot on D−2,
  the newest day complete at T (notebook 04, section 5.3).
#}
select
    s.decision_time,
    s.target_day,
    s.slot_start,
    d2.value_mw as solar_d2_mw
from {{ ref('dws_decision_slots') }} as s
left join {{ ref('dwd_tepco_actuals') }} as d2
    on d2.measure = 'solar'
    and d2.slot_start = timestamp_sub(s.slot_start, interval 2 day)
    and {{ as_of('d2', 's.decision_time') }}

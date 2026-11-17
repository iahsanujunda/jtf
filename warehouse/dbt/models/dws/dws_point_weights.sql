{#
  Each weather point's weight at decision time T: its part of the TEPCO area's installed solar in
  the newest release known at T (its prefecture, or Shizuoka east of the Fuji River for numazu).
  Weights are data with their own known_at, not settings (PRD), so they are read through as_of.
  They weight clear-sky radiation here and, later, the weather at the points. Exact arithmetic
  (NUMERIC), so the same T always gives the same weights.
#}

with decisions as (
    select distinct decision_time
    from {{ ref('dws_decision_slots') }}
),

figures as (
    select
        d.decision_time,
        v.ref_date,
        v.point,
        cast(v.solar_mw as numeric) as mw
    from decisions as d
    inner join {{ ref('dwd_capacity_area') }} as v
        on {{ as_of('v', 'd.decision_time') }}
),

newest as (
    select *
    from figures
    qualify ref_date = max(ref_date) over (partition by decision_time)
)

select
    decision_time,
    point,
    ref_date,
    cast(mw / sum(mw) over (partition by decision_time) as float64) as weight
from newest

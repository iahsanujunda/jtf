{#
  Installed solar in the TEPCO area on target day D, as estimated at its decision time T: the
  least-squares line through the newest four releases known at T, by the quarter they describe,
  evaluated at D, with the newest release carried as it is beside it. Carrying runs about 2% low
  as the fleet grows; the line is about four times closer (notebooks/03_solar_capacity.ipynb,
  section 7). Releases are ordered by quarter, never by known_at: end of June 2017 is known only
  from 2024 (notebooks/04_ods_to_dwd.ipynb, section 6.5). One row per decision time, read by the
  features, the metrics' normalisation and the range checks. Reads DWD only through as_of.

  Exact arithmetic (NUMERIC) throughout, so the same T always gives the same figure to the bit.
  x is days from D, so the line's value at D is its intercept.
#}

with decisions as (
    select distinct decision_time, target_day
    from {{ ref('dws_decision_slots') }}
),

-- Each release's figure for the whole area, as known at T: the sum of its parts' current versions.
releases_at_t as (
    select
        d.decision_time,
        d.target_day,
        v.ref_date,
        sum(cast(v.solar_mw as numeric)) as area_mw,
        max(v.known_at) as known_at
    from decisions as d
    inner join {{ ref('dwd_capacity_area') }} as v
        on {{ as_of('v', 'd.decision_time') }}
    group by d.decision_time, d.target_day, v.ref_date
),

newest_four as (
    select
        *,
        cast(date_diff(ref_date, target_day, day) as numeric) as x
    from releases_at_t
    qualify row_number() over (partition by decision_time order by ref_date desc) <= 4
),

fit as (
    select
        decision_time,
        count(*) as releases,
        max(ref_date) as newest_ref_date,
        min(ref_date) as oldest_ref_date,
        max(known_at) as newest_known_at,
        any_value(area_mw having max ref_date) as carried_mw,
        sum(x) as sx,
        sum(area_mw) as sy,
        sum(x * x) as sxx,
        sum(x * area_mw) as sxy
    from newest_four
    group by decision_time
),

line as (
    select
        *,
        safe_divide(releases * sxy - sx * sy, releases * sxx - sx * sx) as slope_mw_per_day
    from fit
)

select
    d.decision_time,
    d.target_day,
    cast(
        if(l.releases = 4, (l.sy - l.slope_mw_per_day * l.sx) / l.releases, null) as float64
    ) as capacity_mw,
    cast(l.carried_mw as float64) as capacity_carried_mw,
    cast(if(l.releases = 4, l.slope_mw_per_day, null) as float64) as capacity_slope_mw_per_day,
    l.newest_ref_date,
    l.oldest_ref_date,
    l.newest_known_at,
    l.releases
from decisions as d
left join line as l using (decision_time)

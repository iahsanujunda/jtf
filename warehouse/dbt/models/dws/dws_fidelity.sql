{#
  How exact each decision time's inputs are: per decision time and input, how many versions T read
  and how many of them are approximate (notebooks/06_warehouse_against_sources.ipynb, section 3;
  PRD, Fidelity). Fidelity comes from what T actually read, not from a date: just after capture
  start, T can still read history.

  - actuals_d2: the versions of D−2's slots current at T, which persistence reads. A rule-dated
    version is approximate: its value is today's, any later correction built in
  - weather: what dws_weather serves at T. Only the live capture has a capture start, so the
    history sources (Previous Runs, Single Runs) are approximate
  - capacity: the releases dws_capacity's line goes through, as known at T. A rule-dated release
    whose files are unchanged since its known_at is exact: its figures are the published ones and
    the rule never dates it early, so it is as precise as a capture

  A run is exact at T only where every input its model reads is (ads_runs). The seven days of
  actuals the ratio baseline reads join with that feature, in step 7.
#}

with decisions as (
    select distinct decision_time, target_day from {{ ref('dws_decision_slots') }}
),

actuals_d2 as (
    select
        d.decision_time,
        d.target_day,
        'actuals_d2' as input,
        count(*) as versions_read,
        countif(v.basis = 'rule') as approximate_versions
    from decisions as d
    inner join {{ ref('dwd_tepco_actuals') }} as v
        on
            v.measure = 'solar'
            and date(v.slot_start, 'Asia/Tokyo') = date_sub(d.target_day, interval 2 day)
            and {{ as_of('v', 'd.decision_time') }}
    group by d.decision_time, d.target_day
),

weather as (
    select
        decision_time,
        target_day,
        'weather' as input,
        count(*) as versions_read,
        countif(source != 'live') as approximate_versions
    from {{ ref('dws_weather') }}
    group by decision_time, target_day
),

capacity as (
    select
        c.decision_time,
        c.target_day,
        'capacity' as input,
        count(*) as versions_read,
        countif(
            r.basis = 'rule'
            and not r.known_at >= greatest(r.a_last_modified, r.b_last_modified)
        ) as approximate_versions
    from {{ ref('dws_capacity') }} as c
    inner join {{ ref('dwd_capacity_releases') }} as r
        on
            r.ref_date between c.oldest_ref_date and c.newest_ref_date
            and {{ as_of('r', 'c.decision_time') }}
    group by c.decision_time, c.target_day
),

inputs as (
    select * from actuals_d2
    union all
    select * from weather
    union all
    select * from capacity
)

select
    decision_time,
    target_day,
    input,
    versions_read,
    approximate_versions,
    if(approximate_versions = 0, 'exact', 'approximate') as fidelity
from inputs

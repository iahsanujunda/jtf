{#
  A day, and the groups a forecast's results are read by: how clear it was, whether it was
  curtailed, its season (plan, Model and evaluation, Segments;
  notebooks/09_output_against_clear_sky.ipynb, section 6). All of it is known only afterwards,
  from the labels and from the days after the day itself: for reading results, never an input.
  It lives in ADS, which nothing below may read.

  - day_ratio: the day's label over its clear-sky output, across its lit slots (clear sky at
    least 5% of capacity). Summed exactly (NUMERIC), so a rebuild gives the same figure.
  - clear_ratio: what a clear day gives at that time of year, the 90th percentile of day_ratio
    over the 29 days around the day, 14 either side.
  - clearness: day_ratio over clear_ratio. sky: clear from 0.9, cloudy under 0.5, mixed between.
  - clear_ratio, clearness and sky are set only once all 14 later days are whole, and never
    change after. Set sooner, a day would move between groups as later days arrived: of the
    newest days, 5.7% ended in another group than the one they started in.
  - is_curtailed: some slot of the day was curtailed. season: by three months, winter being
    December to February.

  A day has a row once each of its 48 slots has a label.
#}
with slots as (
    select
        f.target_day as day,
        l.label_mw,
        l.curtailed_mw,
        f.clear_sky_mw,
        f.clear_sky_mw >= 0.05 * f.capacity_mw as is_lit
    from {{ ref('dws_solar_features') }} as f
    inner join {{ ref('ads_ml_label_solar') }} as l on l.slot_start = f.slot_start
),

days as (
    select
        day,
        cast(
            safe_divide(
                sum(if(is_lit, cast(label_mw as numeric), null)),
                sum(if(is_lit, cast(clear_sky_mw as numeric), null))
            ) as float64
        ) as day_ratio,
        logical_or(curtailed_mw > 0) as is_curtailed
    from slots
    group by day
    having count(label_mw) = 48
),

around as (
    select distinct
        d.day,
        percentile_cont(n.day_ratio, 0.9) over (partition by d.day) as clear_ratio,
        countif(n.day > d.day) over (partition by d.day) as later_days
    from days as d
    inner join days as n
        on n.day between date_sub(d.day, interval 14 day) and date_add(d.day, interval 14 day)
),

settled as (
    select
        d.day,
        d.day_ratio,
        d.is_curtailed,
        if(a.later_days = 14, a.clear_ratio, null) as clear_ratio,
        if(a.later_days = 14, safe_divide(d.day_ratio, a.clear_ratio), null) as clearness
    from days as d
    inner join around as a on a.day = d.day
)

select
    day,
    day_ratio,
    clear_ratio,
    clearness,
    case
        when clearness is null then null
        when clearness >= 0.9 then 'clear'
        when clearness < 0.5 then 'cloudy'
        else 'mixed'
    end as sky,
    is_curtailed,
    case
        when extract(month from day) in (12, 1, 2) then 'winter'
        when extract(month from day) in (3, 4, 5) then 'spring'
        when extract(month from day) in (6, 7, 8) then 'summer'
        else 'autumn'
    end as season
from settled

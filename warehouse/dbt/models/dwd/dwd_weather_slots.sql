{#
  The three weather sources on DWD's 30-minute slots in JST (plan, DWD; notebooks/04_ods_to_dwd.ipynb,
  section 3.5): per source, point, model, lead or run or capture, and slot. Both slots of an hour
  take the radiation stamped at its end, the mean over that hour; cloud cover, temperature and wind
  are interpolated linearly to the slot's middle, between the hour at or before it (a) and the next
  (b). So a slot needs hours a and b of the same lead, run or capture.

  Versions: a slot is known once both its hours are (b alone where a is a hole), and changes when
  either does; a change that leaves the slot's values as they were is no new version. A version's
  basis is captured when either hour's version is. No hole is filled here: a slot whose hour b is
  missing has no row, and one whose hour a is missing has radiation only (section 3.6; DWS fills).
#}
{{ config(
    partition_by={'field': 'slot_start', 'data_type': 'timestamp', 'granularity': 'month'},
    cluster_by=['source', 'model', 'point'],
) }}

{% set variables = weather_variables() %}

with hours as (
    select
        'previous_runs' as source, point, model, lead_days,
        cast(null as timestamp) as run_time, cast(null as timestamp) as captured_at,
        valid_time, {{ variables | join(', ') }}, known_at, known_until, basis
    from {{ ref('dwd_weather_previous_runs') }}
    union all
    select
        'single_runs', point, model, null, run_time, null,
        valid_time, {{ variables | join(', ') }}, known_at, known_until, basis
    from {{ ref('dwd_weather_single_runs') }}
    union all
    select
        'live', point, model, null, null, captured_at,
        valid_time, {{ variables | join(', ') }}, known_at, known_until, basis
    from {{ ref('dwd_weather_live') }}
),

-- One series per source, point, model and lead, run or capture: its hours belong together.
series_hours as (
    select
        *,
        concat(
            source, '|', point, '|', model, '|', coalesce(cast(lead_days as string), ''), '|',
            coalesce(string(run_time), ''), '|', coalesce(string(captured_at), '')
        ) as series
    from hours
),

first_known as (
    select series, valid_time, min(known_at) as first_known_at
    from series_hours
    group by series, valid_time
),

-- The two slots of each hour b's interval [b − 1 h, b), wherever hour b exists.
slots as (
    select
        f.series,
        slot_start,
        timestamp_sub(f.valid_time, interval 1 hour) as hour_a,
        f.valid_time as hour_b,
        if(extract(minute from slot_start) = 0, 0.25, 0.75) as weight_b,
        greatest(f.first_known_at, coalesce(a.first_known_at, f.first_known_at)) as known_from
    from first_known as f
    cross join unnest([
        timestamp_sub(f.valid_time, interval 60 minute),
        timestamp_sub(f.valid_time, interval 30 minute)
    ]) as slot_start
    left join first_known as a
        on a.series = f.series and a.valid_time = timestamp_sub(f.valid_time, interval 1 hour)
),

-- Every time a slot may change: when it becomes known, and each later version of hour a or b.
events as (
    select distinct s.series, s.slot_start, greatest(h.known_at, s.known_from) as event_at
    from slots as s
    cross join unnest([s.hour_a, s.hour_b]) as hour
    inner join series_hours as h on h.series = s.series and h.valid_time = hour
),

valued as (
    select
        e.series,
        e.slot_start,
        e.event_at as known_at,
        b.source, b.point, b.model, b.lead_days, b.run_time, b.captured_at,
        b.ghi_w_m2,
        b.direct_w_m2,
        b.diffuse_w_m2,
        {%- for v in ['cloud_cover_pct', 'temperature_c', 'wind_speed_kmh'] %}
        a.{{ v }} + (b.{{ v }} - a.{{ v }}) * s.weight_b as {{ v }},
        {%- endfor %}
        b.basis = 'captured' or coalesce(a.basis = 'captured', false) as captured
    from events as e
    inner join slots as s on s.series = e.series and s.slot_start = e.slot_start
    inner join series_hours as b
        on b.series = e.series and b.valid_time = s.hour_b and {{ as_of('b', 'e.event_at') }}
    left join series_hours as a
        on a.series = e.series and a.valid_time = s.hour_a and {{ as_of('a', 'e.event_at') }}
),

changes as (
    select
        *,
        to_json_string(struct({{ variables | join(', ') }})) as reading,
        lag(to_json_string(struct({{ variables | join(', ') }}))) over (
            partition by series, slot_start order by known_at
        ) as previous_reading
    from valued
),

kept as (
    select *
    from changes
    where previous_reading is null or reading != previous_reading
)

select
    source,
    point,
    model,
    lead_days,
    run_time,
    captured_at,
    slot_start,
    {{ variables | join(',\n    ') }},
    known_at,
    lead(known_at) over (partition by series, slot_start order by known_at) as known_until,
    if(captured, 'captured', 'rule') as basis
from kept

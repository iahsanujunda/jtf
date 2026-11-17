{#
  The sun at each weather point, per 30-minute slot: its geometric height at the slot's two edges,
  and Haurwitz clear-sky radiation as the mean of six samples at the slot's 5-minute middles
  (one sample at the middle reads 22% low at dawn and dusk). Computed from time and place alone:
  no source, no known_at, and a fixed range, so nothing reads the clock. The six samples are added
  in a fixed order, because BigQuery doesn't promise the same SUM of floats twice.
  Designed in notebooks/05_clear_sky.ipynb.
#}
{{
    config(
        partition_by={'field': 'slot_start', 'data_type': 'timestamp', 'granularity': 'month'},
        cluster_by=['point', 'slot_start'],
    )
}}

with slots as (
    select
        timestamp_add(timestamp(day, 'Asia/Tokyo'), interval 30 * slot minute) as slot_start
    from
        unnest(generate_date_array(
            date('{{ var("sun_first_day") }}'), date('{{ var("sun_last_day") }}')
        )) as day,
        unnest(generate_array(0, 47)) as slot
),

-- The slot's two edges, and six samples at the middles of its 5-minute steps (seconds in)
samples as (
    select s.slot_start, k.kind, timestamp_add(s.slot_start, interval k.offset_s second) as ts
    from slots as s
    cross join unnest([
        struct('edge_start' as kind, 0 as offset_s),
        struct('edge_end', 1800),
        struct('s0', 150), struct('s1', 450), struct('s2', 750),
        struct('s3', 1050), struct('s4', 1350), struct('s5', 1650)
    ]) as k
),

times as (select distinct ts from samples),

points as (select point, lat, lon from {{ ref('dim_weather_points') }}),

{{ sun_position('times', 'points') }},

by_slot as (
    select
        p.point,
        s.slot_start,
        max(if(s.kind = 'edge_start', p.elevation, null)) as elevation_start,
        max(if(s.kind = 'edge_end', p.elevation, null)) as elevation_end,
        max(if(s.kind = 's0', p.clear_sky_ghi, null)) as s0,
        max(if(s.kind = 's1', p.clear_sky_ghi, null)) as s1,
        max(if(s.kind = 's2', p.clear_sky_ghi, null)) as s2,
        max(if(s.kind = 's3', p.clear_sky_ghi, null)) as s3,
        max(if(s.kind = 's4', p.clear_sky_ghi, null)) as s4,
        max(if(s.kind = 's5', p.clear_sky_ghi, null)) as s5
    from samples as s
    inner join position as p on p.ts = s.ts
    group by p.point, s.slot_start
)

select
    point,
    slot_start,
    elevation_start,
    elevation_end,
    (s0 + s1 + s2 + s3 + s4 + s5) / 6 as clear_sky_ghi
from by_slot

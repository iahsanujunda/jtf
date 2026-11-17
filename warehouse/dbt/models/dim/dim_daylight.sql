{#
  Night and daylight per slot, for the whole area. A slot is night when the sun is below the
  horizon (0°, geometric) at both its edges at all nine points: dark for the whole half hour,
  everywhere. TEPCO shows solar output in none of those slots since February 2024
  (notebooks/05_clear_sky.ipynb, section 2). Daylight is every other slot: the metrics' filter.
#}
{{ config(cluster_by=['slot_start']) }}

select
    slot_start,
    max(greatest(elevation_start, elevation_end)) as highest_edge,
    max(greatest(elevation_start, elevation_end)) < 0 as is_night,
    count(*) as points
from {{ ref('dim_sun_slots') }}
group by slot_start

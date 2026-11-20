{#
  TEPCO's forecast for the next day, for its area, as versions: one row per slot, measure and
  version, in MW averaged over the slot (kWh per half hour over 500), keyed by the slot's start.
  A value is known from the first capture that held it, on the evening before the day it
  forecasts: the update time written in the file moves with every capture while the values stay,
  so it dates nothing (notebooks/12_tepco_forecast_as_yardstick.ipynb, section 0). TEPCO keeps
  no archive of this file, so there is no history and no rule: every version is `captured`. The
  same values in a later capture are not a version; a slot left empty is no value at all.

  Published hours after the decision time of the day it forecasts: a yardstick to set a forecast
  against, never an input to one (plan, Point-in-time rules).
#}
{{ config(cluster_by=['measure', 'slot_start']) }}

-- A re-parse appends a file's rows again under a new parser_version: read each file as last parsed.
with latest_parse as (
    select *
    from {{ source('ods', 'tepco_area_forecast_next_day') }}
    qualify ingested_at = max(ingested_at) over (partition by source_file)
),

observed as (
    select
        timestamp(
            datetime(parse_date('%Y%m%d', date), parse_time('%H%M', slot_from)), 'Asia/Tokyo'
        ) as slot_start,
        measure,
        value_kwh / 500 as value_mw,
        fetched_at,
        source_file
    from latest_parse
    unpivot (
        value_kwh for measure in (
            wind_solar_kwh as 'wind_solar', demand_kwh as 'demand', generation_kwh as 'generation'
        )
    )
),

sightings as (
    select
        *,
        lag(value_mw) over by_fetch as previous_mw,
        row_number() over by_fetch as sighting
    from observed
    window by_fetch as (partition by slot_start, measure order by fetched_at, source_file)
)

select
    slot_start,
    measure,
    cast(value_mw as float64) as value_mw,
    fetched_at as known_at,
    lead(fetched_at) over (partition by slot_start, measure order by fetched_at) as known_until,
    'captured' as basis,
    fetched_at as first_seen,
    source_file,
    sighting > 1 as correction
from sightings
where sighting = 1 or value_mw != previous_mw

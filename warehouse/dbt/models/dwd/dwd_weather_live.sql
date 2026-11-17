{#
  The live forecast captured daily at 08:45 JST: per point, model, capture and hour. Known at the
  fetch, since the response doesn't say which run it holds (notebooks/04_ods_to_dwd.ipynb, section
  3.3). Each capture is a forecast of its own, never a version of another, so captures are kept
  apart like runs and known_until is always null; DWS picks the newest one before T. Each file as
  last parsed.
#}
{{ config(cluster_by=['model', 'captured_at', 'point']) }}

with latest_parse as (
    select *
    from {{ source('ods', 'openmeteo_live') }}
    qualify ingested_at = max(ingested_at) over (partition by source_file)
)

select
    o.point,
    o.model,
    o.fetched_at as captured_at,
    o.valid_time,
    {{ weather_from_ods('o') }},
    o.fetched_at as known_at,
    cast(null as timestamp) as known_until,
    'captured' as basis,
    o.source_file
from latest_parse as o

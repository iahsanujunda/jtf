{#
  Every daily capture, one row per source and raw file fetched, known when it was fetched. It is
  where capture times are read, so nothing above DWD reads the ODS manifest: each versioned model
  takes its source's capture start from it (the first daily capture, test runs included: they are
  real captures, notebooks/04_ods_to_dwd.ipynb, section 1), and the decision-time spine its newest
  capture. A capture is a fact of its own, never a version of another, so known_until is always
  null. The manifest has a row per file and ODS table, and per parse: one row per fetch here.
#}
select distinct
    source,
    path as source_file,
    extraction_date,
    fetched_at,
    fetched_at as known_at,
    cast(null as timestamp) as known_until
from {{ source('ods', 'raw_files') }}
where load_type = 'daily'

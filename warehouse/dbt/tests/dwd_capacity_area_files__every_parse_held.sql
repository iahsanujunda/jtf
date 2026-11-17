-- The incremental model holds every capacity file at its newest parse: a file it missed, or holds
-- at an older parse, is a row here. A full rebuild would hold them all.
{{ config(group='capacity_figures', meta={'dagster': {'ref': {'name': 'dwd_capacity_area_files'}}}) }}

with newest as (
    select path as source_file, max(ingested_at) as parsed_at
    from {{ source('ods', 'raw_files') }}
    where ods_table in ('meti_fit_capacity_pref', 'meti_fit_capacity_city') and status = 'parsed'
    group by path
)

select n.*
from newest as n
where not exists (
    select 1 from {{ ref('dwd_capacity_area_files') }} as f
    where f.source_file = n.source_file and f.parsed_at = n.parsed_at
)

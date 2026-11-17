-- Every file holds every part of its table, each from all of its METI areas: a prefecture or a
-- municipality renamed in a release would otherwise drop out of the sum without a trace.
{{ config(meta={'dagster': {'ref': {'name': 'dwd_capacity_area_files'}}}) }}

with parts as (
    select meti_table, point, count(*) as meti_areas
    from {{ ref('tepco_area_parts') }}
    group by meti_table, point
),

files as (
    select distinct source_file, meti_table from {{ ref('dwd_capacity_area_files') }}
)

select files.source_file, parts.point, parts.meti_areas as expected, f.meti_areas as found
from files
inner join parts using (meti_table)
left join {{ ref('dwd_capacity_area_files') }} as f
    on f.source_file = files.source_file and f.point = parts.point
where f.meti_areas is null or f.meti_areas != parts.meti_areas

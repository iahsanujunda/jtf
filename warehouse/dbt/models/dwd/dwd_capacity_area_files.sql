{#
  Installed solar in each part of the TEPCO area, per raw capacity file as last parsed: new and
  transferred installations, under and over 10 kW, in MW (METI's kW ÷ 1,000). A part is named by its weather point:
  its prefecture from table A, or Shizuoka east of the Fuji River (numazu) from table B
  (seed tepco_area_parts). The only model that reads the 2 GB of capacity figures, so it is
  incremental: each run reads only the files whose newest parse it doesn't hold, which covers new
  files, re-parses under a new parser version, and parses that finished out of order. Versions
  and known_at are dwd_capacity_area's. A change to its columns needs one build with
  --full-refresh; until then its runs fail (on_schema_change: fail) rather than merge rows of two
  shapes. Designed in notebooks/04_ods_to_dwd.ipynb, sections 6.7
  and 6.8.
#}
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['source_file', 'point'],
    on_schema_change='fail',
) }}

{% set files_sql %}
    with parsed as (
        select path as source_file, extraction_date, max(ingested_at) as parsed_at
        from {{ source('ods', 'raw_files') }}
        where ods_table in ('meti_fit_capacity_pref', 'meti_fit_capacity_city')
            and status = 'parsed'
        group by path, extraction_date
    )

    select * from parsed
    {% if is_incremental() %}
        where not exists (
            select 1 from {{ this }} as held
            where held.source_file = parsed.source_file and held.parsed_at = parsed.parsed_at
        )
    {% endif %}
{% endset %}

with files as (
    {{ files_sql }}
),

figures as (
    select 'pref' as meti_table, source_file, ref_date, area, value, fetched_at, ingested_at
    from {{ source('ods', 'meti_fit_capacity_pref') }}
    where sheet in ('表A②－１', '表A②－２')
        and path in ('太陽光発電設備/10kW未満', '太陽光発電設備/10kW以上')
        and source_file in (select source_file from files)
        {{ only_files(files_sql) }}
    union all
    select 'city' as meti_table, source_file, ref_date, area, value, fetched_at, ingested_at
    from {{ source('ods', 'meti_fit_capacity_city') }}
    where sheet in ('表B②－１', '表B②－２')
        and path in ('太陽光発電設備/10kW未満', '太陽光発電設備/10kW以上')
        and source_file in (select source_file from files)
        {{ only_files(files_sql) }}
),

-- A re-parse appends a file's rows again under a new parser_version: read each file as last parsed.
latest_parse as (
    select *
    from figures
    qualify ingested_at = max(ingested_at) over (partition by source_file)
)

select
    f.source_file,
    p.point,
    any_value(f.ref_date) as ref_date,
    any_value(f.meti_table) as meti_table,
    any_value(f.fetched_at) as fetched_at,
    any_value(files.parsed_at) as parsed_at,
    count(distinct f.area) as meti_areas,
    -- Summed exactly: a float sum depends on the order BigQuery adds in, so the same files could
    -- give figures a few 1e-10 kW apart, and dwd_capacity_area would see a correction.
    cast(sum(cast(f.value as numeric)) / 1000 as float64) as solar_mw
from latest_parse as f
inner join {{ ref('tepco_area_parts') }} as p
    on p.meti_table = f.meti_table and p.meti_area = f.area
inner join files using (source_file)
group by f.source_file, p.point

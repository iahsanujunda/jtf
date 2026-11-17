-- The history rule dates only what we could not have captured: no rule-dated release may be known
-- at or after the source's capture start.
{{ config(meta={'dagster': {'ref': {'name': 'dwd_capacity_releases'}}}) }}

with capture_start as (
    select min(fetched_at) as t
    from {{ source('ods', 'raw_files') }}
    where source = 'meti_fit_capacity' and load_type = 'daily'
)

select r.*
from {{ ref('dwd_capacity_releases') }} as r
where r.basis = 'rule' and r.known_at >= (select t from capture_start)

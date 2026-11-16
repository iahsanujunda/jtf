-- Nothing can be known after the newest file we hold of it.
{{ config(meta={'dagster': {'ref': {'name': 'dwd_tepco_actuals'}}}) }}

with newest as (
    select max(fetched_at) as t
    from {{ source('ods', 'raw_files') }}
    where source = 'tepco_actuals'
)

select v.*
from {{ ref('dwd_tepco_actuals') }} as v
where v.known_at > (select t from newest)

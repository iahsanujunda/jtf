-- The history rule dates only what we could not have captured: no rule-dated version may be known
-- at or after the source's capture start.
with capture_start as (
    select min(fetched_at) as t
    from {{ source('ods', 'raw_files') }}
    where source = 'tepco_actuals' and load_type = 'daily'
)

select v.*
from {{ ref('dwd_tepco_actuals') }} as v
where v.basis = 'rule' and v.known_at >= (select t from capture_start)

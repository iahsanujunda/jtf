-- No figure is known before its release: a release is usable only once both its tables are held.
{{ config(meta={'dagster': {'ref': {'name': 'dwd_capacity_area'}}}) }}

select a.*, r.known_at as release_known_at
from {{ ref('dwd_capacity_area') }} as a
inner join {{ ref('dwd_capacity_releases') }} as r using (ref_date)
where a.known_at < r.known_at

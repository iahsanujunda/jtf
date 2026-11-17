-- Every release dated in dwd_capacity_releases has a current figure for every part of the area.
{{ config(meta={'dagster': {'ref': {'name': 'dwd_capacity_area'}}}) }}

with current_figures as (
    select ref_date, count(*) as parts
    from {{ ref('dwd_capacity_area') }}
    where known_until is null
    group by ref_date
)

select r.ref_date, c.parts
from {{ ref('dwd_capacity_releases') }} as r
left join current_figures as c using (ref_date)
where coalesce(c.parts, 0) != (select count(distinct point) from {{ ref('tepco_area_parts') }})

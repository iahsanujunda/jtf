-- The spine runs to the day after the newest daily capture in JST, counted here from ODS itself,
-- not from dwd_source_captures, which the spine reads: built from a stale copy of the captures,
-- the spine is consistently a day or more short, and every check of it agreeing with itself
-- passes.
{{ config(meta={'dagster': {'ref': {'name': 'dws_decision_slots'}}}) }}

with captured as (
    select date_add(date(max(fetched_at), 'Asia/Tokyo'), interval 1 day) as last_day
    from {{ source('ods', 'raw_files') }}
    where load_type = 'daily'
),

spine as (
    select max(target_day) as last_day
    from {{ ref('dws_decision_slots') }}
)

select spine.last_day as spine_last_day, captured.last_day as captured_last_day
from spine cross join captured
where spine.last_day is distinct from captured.last_day

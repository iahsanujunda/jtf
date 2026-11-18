{#
  TEPCO's monthly area actuals as versions: one row per slot, measure and version, in MW
  averaged over the slot, keyed by the slot's start. A changed value is a new version; the same
  value seen again is not. A first version takes the history rule (slot end + 1 hour) when that
  time falls before the source's capture start, its first daily capture; every other version takes
  the fetch time of the first file holding it. Designed in notebooks/04_ods_to_dwd.ipynb, section 2.
#}
{{ config(cluster_by=['measure', 'slot_start']) }}

with capture_start as (
    select min(known_at) as t
    from {{ ref('dwd_source_captures') }}
    where source = 'tepco_actuals'
),

-- A re-parse appends a file's rows again under a new parser_version: read each file as last parsed.
latest_parse as (
    select *
    from {{ source('ods', 'tepco_actuals') }}
    qualify ingested_at = max(ingested_at) over (partition by source_file)
),

observed as (
    select
        timestamp(
            datetime(parse_date('%Y/%m/%d', date), parse_time('%H:%M', time)), 'Asia/Tokyo'
        ) as slot_start,
        measure,
        value_mw,
        fetched_at,
        source_file
    from latest_parse
    unpivot include nulls (value_mw for measure in (solar, solar_curtailed, wind))
),

sightings as (
    select
        *,
        lag(value_mw) over by_fetch as previous_mw,
        row_number() over by_fetch as sighting
    from observed
    window by_fetch as (partition by slot_start, measure order by fetched_at, source_file)
),

versions as (
    select
        slot_start,
        measure,
        value_mw,
        source_file,
        fetched_at as first_seen,
        timestamp_add(slot_start, interval 90 minute) as rule_time,
        sighting > 1 as correction,
        sighting = 1
            and timestamp_add(slot_start, interval 90 minute) < (select t from capture_start)
            as by_rule
    from sightings
    where sighting = 1 or value_mw is distinct from previous_mw
),

dated as (
    select
        *,
        if(by_rule, rule_time, first_seen) as known_at,
        if(by_rule, 'rule', 'captured') as basis
    from versions
)

select
    slot_start,
    measure,
    cast(value_mw as float64) as value_mw,
    known_at,
    lead(known_at) over (partition by slot_start, measure order by known_at) as known_until,
    basis,
    first_seen,
    source_file,
    correction
from dated

{#
  JEPX's day-ahead spot prices as versions: one row per delivery slot, measure and version, in
  yen per kWh, keyed by the slot's start (slot 1 starts at 00:00 JST). Two measures: the Tokyo
  area price and the system price. A changed price is a new version; the same price seen again
  is not. None has been seen to change (notebooks/12_tepco_forecast_as_yardstick.ipynb,
  section 5), and a correction would show here if one came. A first version takes the history
  rule, 10:30 JST on the day before delivery (bidding closes at 10:00), when that time falls
  before the source's capture start, its first daily capture; every other version takes the
  fetch time of the first file holding it, 11:00 on the day before in the daily capture.

  So the price of day D is never known at 09:00 on D−1: a forecast for D can read prices up to
  D−1's (plan, Point-in-time rules).
#}
{{ config(cluster_by=['measure', 'slot_start']) }}

with capture_start as (
    select min(known_at) as t
    from {{ ref('dwd_source_captures') }}
    where source = 'jepx_spot'
),

-- A re-parse appends a file's rows again under a new parser_version: read each file as last parsed.
latest_parse as (
    select *
    from {{ source('ods', 'jepx_spot') }}
    qualify ingested_at = max(ingested_at) over (partition by source_file)
),

observed as (
    select
        timestamp_add(
            timestamp(parse_date('%Y/%m/%d', delivery_date), 'Asia/Tokyo'),
            interval (slot_code - 1) * 30 minute
        ) as slot_start,
        timestamp(
            datetime(
                date_sub(parse_date('%Y/%m/%d', delivery_date), interval 1 day), time '10:30:00'
            ),
            'Asia/Tokyo'
        ) as rule_time,
        measure,
        value_yen_kwh,
        fetched_at,
        source_file
    from latest_parse
    unpivot (
        value_yen_kwh for measure in (
            price_tokyo_yen_kwh as 'price_tokyo', system_price_yen_kwh as 'price_system'
        )
    )
),

sightings as (
    select
        *,
        lag(value_yen_kwh) over by_fetch as previous_yen_kwh,
        row_number() over by_fetch as sighting
    from observed
    window by_fetch as (partition by slot_start, measure order by fetched_at, source_file)
),

versions as (
    select
        slot_start,
        measure,
        value_yen_kwh,
        source_file,
        fetched_at as first_seen,
        sighting > 1 as correction,
        rule_time,
        sighting = 1 and rule_time < (select t from capture_start) as by_rule
    from sightings
    where sighting = 1 or value_yen_kwh != previous_yen_kwh
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
    value_yen_kwh,
    known_at,
    lead(known_at) over (partition by slot_start, measure order by known_at) as known_until,
    basis,
    first_seen,
    source_file,
    correction
from dated

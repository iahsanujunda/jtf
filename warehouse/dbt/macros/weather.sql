{#
  Weather in DWD: the six Open-Meteo variables under DWD's names, with their units in the name
  (radiation in W/m² as the mean of the hour before its stamp, cloud cover in %, temperature in °C,
  wind in km/h, as Open-Meteo gives them and the parser asserts).
#}
{% macro weather_variables() -%}
    {{ return(['ghi_w_m2', 'direct_w_m2', 'diffuse_w_m2', 'cloud_cover_pct', 'temperature_c', 'wind_speed_kmh']) }}
{%- endmacro %}

{% macro weather_from_ods(alias) -%}
    {{ alias }}.shortwave_radiation as ghi_w_m2,
    {{ alias }}.direct_radiation as direct_w_m2,
    {{ alias }}.diffuse_radiation as diffuse_w_m2,
    {{ alias }}.cloud_cover as cloud_cover_pct,
    {{ alias }}.temperature_2m as temperature_c,
    {{ alias }}.wind_speed_10m as wind_speed_kmh
{%- endmacro %}

{#
  Versions of hourly weather from a history-only Open-Meteo table, one row per point, model, `key`
  (the lead or the run) and hour (`valid_time`, UTC). Each file is read as last parsed; a copy seen
  again is no new version; a changed value is (plan, point-in-time rules 1 and 2). These sources
  have no daily capture, so no capture start: a first version is known by `rule`, an expression
  over the ODS row `o` and the model's delay `w.delay_hours`, with basis rule. A later version is
  known at the fetch that first held it, never before the version it replaces, with basis captured.
  Versions sharing a known_at keep the newest copy, so as_of finds one per key at any time.
#}
{% macro weather_versions(table, key, rule) %}
with latest_parse as (
    select *
    from {{ source('ods', table) }}
    qualify ingested_at = max(ingested_at) over (partition by source_file)
),

copies as (
    select
        o.point,
        o.model,
        o.{{ key }},
        o.valid_time,
        {{ weather_from_ods('o') }},
        o.fetched_at,
        o.source_file,
        {{ rule }} as rule_time
    from latest_parse as o
    inner join {{ ref('dim_weather_models') }} as w on w.model = o.model
),

sightings as (
    select
        *,
        to_json_string(struct({{ weather_variables() | join(', ') }})) as reading,
        row_number() over by_fetch as sighting
    from copies
    window by_fetch as (partition by point, model, {{ key }}, valid_time order by fetched_at, source_file)
),

changes as (
    select
        *,
        lag(reading) over (
            partition by point, model, {{ key }}, valid_time order by fetched_at, source_file
        ) as previous_reading
    from sightings
),

versions as (
    select *, sighting > 1 as correction
    from changes
    where sighting = 1 or reading is distinct from previous_reading
),

dated as (
    select
        *,
        max(if(correction, fetched_at, rule_time)) over (
            partition by point, model, {{ key }}, valid_time
            order by fetched_at, source_file
            rows between unbounded preceding and current row
        ) as known_at
    from versions
),

one_per_time as (
    select *
    from dated
    qualify row_number() over (
        partition by point, model, {{ key }}, valid_time, known_at
        order by fetched_at desc, source_file desc
    ) = 1
)

select
    point,
    model,
    {{ key }},
    valid_time,
    {{ weather_variables() | join(',\n    ') }},
    known_at,
    lead(known_at) over (partition by point, model, {{ key }}, valid_time order by known_at)
        as known_until,
    if(correction, 'captured', 'rule') as basis,
    fetched_at as first_seen,
    source_file,
    correction
from one_per_time
{% endmacro %}

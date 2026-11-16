{#
  The versions of a DWD model that were current at decision time `t`: the version with
  known_at <= t and no newer version known by then. Used as a join condition, so one query serves
  one T or every T of a backtest (notebook 04, section 5):

      join {{ ref('dwd_tepco_actuals') }} as v
        on v.slot_start = ... and {{ as_of('v', 's.decision_time') }}

  `versions` is the alias of a model that carries known_at and known_until. DWS reads DWD only
  through this macro; a SQL rule checks it.
#}
{% macro as_of(versions, t) -%}
    ({{ versions }}.known_at <= {{ t }}
     and ({{ versions }}.known_until is null or {{ t }} < {{ versions }}.known_until))
{%- endmacro %}

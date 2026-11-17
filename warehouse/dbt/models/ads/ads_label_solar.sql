{#
  The truth of each slot, which forecasts are scored against: TEPCO's actuals at their latest
  version. Targets take the latest values, never as known at T (plan, point-in-time rule 4), so
  nothing here goes through as_of, and no DWS model may read it (a layer above).
  label_mw is what the weather allowed, delivered plus curtailed (plan, Decisions, "Label on
  curtailed slots"); delivered_mw beside it. Two terms added in a fixed order: the same every build.
#}
{{ config(cluster_by=['slot_start']) }}

with latest as (
    select slot_start, measure, value_mw
    from {{ ref('dwd_tepco_actuals') }}
    where known_until is null and measure in ('solar', 'solar_curtailed')
)

select
    slot_start,
    solar + solar_curtailed as label_mw,
    solar as delivered_mw,
    solar_curtailed as curtailed_mw
from latest
pivot (any_value(value_mw) for measure in ('solar', 'solar_curtailed'))

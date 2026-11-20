{#
  Each prediction set beside TEPCO's own forecast for the same slot, wherever TEPCO's was
  captured: the yardstick (plan, Benchmark; notebooks/12_tepco_forecast_as_yardstick.ipynb).

  TEPCO forecasts wind and solar together, so both sides are made the same thing:
  - forecast_mw: the run's solar forecast plus the same slot's wind two days back, as known at
    the run's decision time (dws_solar_features.wind_d2_mw)
  - tepco_mw: TEPCO's forecast, at its latest version
  - actual_mw: delivered solar plus wind, at their latest versions. Curtailed output is left
    out: TEPCO forecasts what is delivered

  The errors are defined here and nowhere else, as predicted minus actual, and as a share of the
  capacity estimated at the run's decision time. A slot with no actual yet keeps its row, with no
  error. TEPCO's forecast is known from the evening before, ten hours after the run's decision
  time: it is ahead by that much, and nothing here corrects for it. Two terms are added in a
  fixed order, so every build gives the same figures.
#}
{{ config(cluster_by=['run_id', 'slot_start']) }}

with tepco as (
    select slot_start, value_mw as tepco_mw
    from {{ ref('dwd_tepco_forecast_next_day') }}
    where measure = 'wind_solar' and known_until is null
),

latest as (
    select slot_start, measure, value_mw
    from {{ ref('dwd_tepco_actuals') }}
    where known_until is null and measure in ('solar', 'wind')
),

happened as (
    select slot_start, solar + wind as actual_mw
    from latest
    pivot (any_value(value_mw) for measure in ('solar', 'wind'))
)

select
    p.run_id,
    p.model,
    p.mode,
    p.decision_time,
    p.target_day,
    p.slot_start,
    p.predicted_mw,
    f.wind_d2_mw,
    p.predicted_mw + f.wind_d2_mw as forecast_mw,
    t.tepco_mw,
    h.actual_mw,
    c.capacity_mw,
    p.predicted_mw + f.wind_d2_mw - h.actual_mw as error_mw,
    safe_divide(p.predicted_mw + f.wind_d2_mw - h.actual_mw, c.capacity_mw) as error_share,
    t.tepco_mw - h.actual_mw as tepco_error_mw,
    safe_divide(t.tepco_mw - h.actual_mw, c.capacity_mw) as tepco_error_share,
    not d.is_night as is_daylight
from {{ source('ads', 'ads_ml_prediction_solar') }} as p
inner join tepco as t on t.slot_start = p.slot_start
left join {{ ref('dws_solar_features') }} as f
    on f.decision_time = p.decision_time and f.slot_start = p.slot_start
left join happened as h on h.slot_start = p.slot_start
left join {{ ref('dws_capacity') }} as c on c.decision_time = p.decision_time
left join {{ ref('dim_daylight') }} as d on d.slot_start = p.slot_start

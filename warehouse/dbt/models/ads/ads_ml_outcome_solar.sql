{#
  Each prediction met by its label. The error is defined here and nowhere else: backtest metrics
  aggregate this table, and so will accuracy monitoring in phase 2. error_share is the error over
  the capacity estimated at the prediction's decision time (dws_capacity), so errors compare
  across the years the fleet grew. A prediction whose slot has no actual yet keeps its row, with
  no label and no error. is_daylight is the metrics' filter (dim_daylight). sky, is_curtailed_day
  and season are the groups results are read by (ads_ml_day_solar): the target day's, with no sky
  until that day's 14 later days are whole. A prediction with a range (p10_mw, p90_mw) also says
  whether the label fell inside it, and its pinball loss: the mean over its three forecasts, in a
  fixed order. A prediction without a range has neither. price_yen_kwh is the Tokyo spot price of
  the slot at its latest version, what a megawatt-hour was worth there: the weight of the
  price-weighted error (notebooks/12_tepco_forecast_as_yardstick.ipynb, section 5). It is read
  after the fact, as the label is, and is empty for a slot not yet traded.
#}
{{ config(cluster_by=['run_id', 'slot_start']) }}

select
    p.run_id,
    p.model,
    p.mode,
    p.fold,
    p.decision_time,
    p.target_day,
    p.slot_start,
    p.predicted_mw,
    p.p10_mw,
    p.p90_mw,
    l.label_mw,
    p.predicted_mw - l.label_mw as error_mw,
    c.capacity_mw,
    safe_divide(p.predicted_mw - l.label_mw, c.capacity_mw) as error_share,
    not d.is_night as is_daylight,
    l.label_mw between p.p10_mw and p.p90_mw as in_range,
    safe_divide(
        (
            {{ pinball(0.1, 'l.label_mw', 'p.p10_mw') }}
            + {{ pinball(0.5, 'l.label_mw', 'p.predicted_mw') }}
            + {{ pinball(0.9, 'l.label_mw', 'p.p90_mw') }}
        ) / 3,
        c.capacity_mw
    ) as pinball_share,
    g.sky,
    g.is_curtailed as is_curtailed_day,
    g.season,
    pr.value_yen_kwh as price_yen_kwh
from {{ source('ads', 'ads_ml_prediction_solar') }} as p
left join {{ ref('ads_ml_label_solar') }} as l on l.slot_start = p.slot_start
left join {{ ref('dws_capacity') }} as c on c.decision_time = p.decision_time
left join {{ ref('dim_daylight') }} as d on d.slot_start = p.slot_start
left join {{ ref('ads_ml_day_solar') }} as g on g.day = p.target_day
left join {{ ref('dwd_jepx_spot') }} as pr
    on pr.slot_start = p.slot_start and pr.measure = 'price_tokyo' and pr.known_until is null

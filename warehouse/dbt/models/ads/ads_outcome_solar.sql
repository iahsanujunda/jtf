{#
  Each prediction met by its label. The error is defined here and nowhere else: backtest metrics
  aggregate this table, and so will accuracy monitoring in phase 2. error_share is the error over
  the capacity estimated at the prediction's decision time (dws_capacity), so errors compare
  across the years the fleet grew. A prediction whose slot has no actual yet keeps its row, with
  no label and no error. is_daylight is the metrics' filter (dim_daylight).
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
    l.label_mw,
    p.predicted_mw - l.label_mw as error_mw,
    c.capacity_mw,
    safe_divide(p.predicted_mw - l.label_mw, c.capacity_mw) as error_share,
    not d.is_night as is_daylight
from {{ source('ads', 'ads_prediction_solar') }} as p
left join {{ ref('ads_label_solar') }} as l on l.slot_start = p.slot_start
left join {{ ref('dws_capacity') }} as c on c.decision_time = p.decision_time
left join {{ ref('dim_daylight') }} as d on d.slot_start = p.slot_start

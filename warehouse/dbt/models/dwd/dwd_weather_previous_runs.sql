{#
  Open-Meteo Previous Runs as versions: per point, model, lead (0 to 2 days) and hour (UTC, the
  stamp Open-Meteo gives; radiation is the mean of the hour before it). A value for lead N is known
  by the history rule, its hour − 24 h × N + the model's delay (plan, point-in-time rules); a later
  fetch that changed it, which happened only to lead 0 of the hours just before the first fetch, is
  a second version known at that fetch (notebooks/04_ods_to_dwd.ipynb, sections 3.1 and 3.4).
  Each model as the source gave it: a hole stays a missing hour (section 3.6); DWS fills it.
  Features use lead 2 only; leads 0 and 1 are kept for the leak scenario F2.
#}
{{ config(cluster_by=['model', 'lead_days', 'point']) }}

{{ weather_versions(
    'openmeteo_previous_runs',
    'lead_days',
    "timestamp_add(timestamp_sub(o.valid_time, interval 24 * o.lead_days hour), interval w.delay_hours hour)",
) }}

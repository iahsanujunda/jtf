{#
  Open-Meteo Single Runs as versions: per point, model, run (its start, UTC) and hour. A run is
  known its model's delay after it starts (MSM 3 h, GSM and ECMWF 6 h, dim_weather_models), assumed
  until the live captures measure it (plan, point-in-time rules). Runs are never collapsed: DWS
  picks one per target day (notebooks/04_ods_to_dwd.ipynb, section 3.2). A run's first hour or two
  of radiation are null by the source's own nature, not holes (section 3.6).
#}
{{ config(cluster_by=['model', 'run_time', 'point']) }}

{{ weather_versions(
    'openmeteo_single_runs',
    'run_time',
    "timestamp_add(o.run_time, interval w.delay_hours hour)",
) }}

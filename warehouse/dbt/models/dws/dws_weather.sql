{#
  The weather known at decision time T for every slot of its target day D, per point and model:
  one source per target day, each hole filled from another model of that source.

  What each source offers at T (DWD through as_of):
  - live: the newest capture known at T. Only the capture on D−1 reaches all of D
  - previous_runs: lead 2, the only lead known at T for all of D (notebooks/04_ods_to_dwd.ipynb, 3.1)
  - single_runs: per model, the newest run known strictly before T. Under the assumed delays the
    newer run of D−2 lands exactly at T, too fine an edge to trust (section 3.2), so the older one
    serves, or an older still after missing runs

  Within a source, a model's missing value is taken from the first other model, by fill_order
  (MSM, ECMWF, GSM), that has it for the same slot and point: at lead 2 one always does (section
  3.6). Radiation is never filled for a model that has none (GSM). Each value records the model it
  came from, in <variable>_model.

  The source is the first, in the order live, previous_runs, single_runs, that after the fill has
  every variable of every model at every point in every daylight slot of D. Live first: a capture
  can only be added before T, never after, so a later top-up of a history source can't change
  which weather a past forecast saw (the backfill-stability test of step 6). A target day no source
  covers gets no rows, and the data test that every daylight slot has weather fails for it.
#}
{{ config(cluster_by=['decision_time', 'model', 'point']) }}

{% set variables = weather_variables() %}
{% set radiation = ['ghi_w_m2', 'direct_w_m2', 'diffuse_w_m2'] %}

with decisions as (
    select distinct decision_time, target_day
    from {{ ref('dws_decision_slots') }}
),

offered as (
    select d.decision_time, s.*
    from decisions as d
    inner join {{ ref('dwd_weather_slots') }} as s
        on date(s.slot_start, 'Asia/Tokyo') = d.target_day
        and {{ as_of('s', 'd.decision_time') }}
    where s.source != 'previous_runs' or s.lead_days = 2
),

newest_capture as (
    select decision_time, model, max(captured_at) as captured_at
    from offered
    where source = 'live'
    group by decision_time, model
),

newest_run as (
    select o.decision_time, o.model, max(o.run_time) as run_time
    from offered as o
    inner join {{ ref('dim_weather_models') }} as w on w.model = o.model
    where o.source = 'single_runs'
        and timestamp_add(o.run_time, interval w.delay_hours hour) < o.decision_time
    group by o.decision_time, o.model
),

chosen_series as (
    select o.*
    from offered as o
    left join newest_capture as c on c.decision_time = o.decision_time and c.model = o.model
    left join newest_run as r on r.decision_time = o.decision_time and r.model = o.model
    where o.source = 'previous_runs'
        or (o.source = 'live' and o.captured_at = c.captured_at)
        or (o.source = 'single_runs' and o.run_time = r.run_time)
),

-- Every slot, point, model and source, so a whole hour missing from a model is a row to fill.
grid as (
    select
        s.decision_time,
        s.target_day,
        s.slot_start,
        p.point,
        w.model,
        w.has_radiation,
        w.fill_order,
        source
    from {{ ref('dws_decision_slots') }} as s
    cross join {{ ref('dim_weather_points') }} as p
    cross join {{ ref('dim_weather_models') }} as w
    cross join unnest(['live', 'previous_runs', 'single_runs']) as source
),

gridded as (
    select
        g.*,
        c.lead_days,
        c.run_time,
        c.captured_at,
        {%- for v in variables %}
        {%- if v in radiation %}
        if(g.has_radiation, c.{{ v }}, null) as {{ v }},
        {%- else %}
        c.{{ v }},
        {%- endif %}
        {%- endfor %}
        array_agg(struct(
            g.model as peer, g.fill_order, {% for v in variables %}c.{{ v }}{{ ', ' if not loop.last }}{% endfor %}
        )) over (partition by g.decision_time, g.source, g.slot_start, g.point) as peers
    from grid as g
    left join chosen_series as c
        on c.decision_time = g.decision_time
        and c.source = g.source
        and c.slot_start = g.slot_start
        and c.point = g.point
        and c.model = g.model
),

filled as (
    select
        * except (peers
            {%- for v in variables %}, {{ v }}{% endfor %}),
        {%- for v in variables %}
        {%- set eligible = 'has_radiation' if v in radiation else 'true' %}
        if({{ eligible }}, coalesce({{ v }}, (
            select p.{{ v }} from unnest(peers) as p
            where p.peer != model and p.{{ v }} is not null
            order by p.fill_order limit 1
        )), null) as {{ v }},
        if({{ eligible }}, if({{ v }} is not null, model, (
            select p.peer from unnest(peers) as p
            where p.peer != model and p.{{ v }} is not null
            order by p.fill_order limit 1
        )), null) as {{ v }}_model{{ ',' if not loop.last }}
        {%- endfor %}
    from gridded
),

-- A source covers D when every daylight slot has every variable each model has, at every point.
coverage as (
    select
        f.decision_time,
        f.source,
        logical_and(
            {%- for v in variables %}
            {%- if v in radiation %}
            (not f.has_radiation or f.{{ v }} is not null)
            {%- else %}
            f.{{ v }} is not null
            {%- endif %}
            {{- ' and' if not loop.last }}
            {%- endfor %}
        ) as complete
    from filled as f
    inner join {{ ref('dim_daylight') }} as d on d.slot_start = f.slot_start and not d.is_night
    group by f.decision_time, f.source
),

chosen as (
    select decision_time, source
    from coverage
    where complete
    qualify row_number() over (
        partition by decision_time
        order by case source when 'live' then 1 when 'previous_runs' then 2 else 3 end
    ) = 1
)

select
    f.decision_time,
    f.target_day,
    f.slot_start,
    f.point,
    f.model,
    f.source,
    f.lead_days,
    f.run_time,
    f.captured_at,
    {%- for v in variables %}
    f.{{ v }},
    {%- endfor %}
    {%- for v in variables %}
    f.{{ v }}_model{{ ',' if not loop.last }}
    {%- endfor %}
from filled as f
inner join chosen as c on c.decision_time = f.decision_time and c.source = f.source

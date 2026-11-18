{#
  The history rule dates only what we could not have captured: no rule-dated version is known at or
  after its source's capture start, the first daily fetch in the ODS manifest. A source with no
  daily capture (the weather history) has no capture start: its rule is checked against the fetch
  that first held each version instead (`basis != 'rule' or known_at <= first_seen`).
#}
{% test rule_before_capture_start(model, source_name) %}
    select *
    from {{ model }}
    where basis = 'rule' and known_at >= (
        select min(fetched_at)
        from {{ source('ods', 'raw_files') }}
        where source = '{{ source_name }}' and load_type = 'daily'
    )
{% endtest %}

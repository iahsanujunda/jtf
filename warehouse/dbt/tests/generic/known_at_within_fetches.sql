{#
  Nothing is known after the newest file we hold of it: no known_at later than the newest fetch of
  the model's sources, read from the ODS manifest.
#}
{% test known_at_within_fetches(model, sources) %}
    select *
    from {{ model }}
    where known_at > (
        select max(fetched_at)
        from {{ source('ods', 'raw_files') }}
        where source in ('{{ sources | join("', '") }}')
    )
{% endtest %}

{#
  Partition and cluster pruning for an incremental model over an ODS table: `and` the fetch days
  and files that `files_sql` lists, written in as constants. BigQuery skips partitions and
  clustered blocks only for constants: filtering by the same list through a subquery or a join
  scans the whole table (notebook 04, section 6.8). ODS is partitioned by `extraction_date` and
  clustered by `source_file`, and the history sits in one partition, so the files do most of it.

  The model must also filter on `files_sql` in SQL, which is what makes it correct; this only
  narrows what is read. It adds nothing on a full build, outside execution, or in a unit test,
  where sources are fixtures and the lookup cannot run. `files_sql` returns `source_file` and
  `extraction_date`; when it returns no rows, nothing is read.
#}
{% macro only_files(files_sql) -%}
    {%- if execute and is_incremental() and model.resource_type != 'unit_test' -%}
        {%- set files = run_query(files_sql) -%}
        {%- if files.rows | length == 0 -%}
            and false
        {%- else -%}
            {%- set days = files.columns['extraction_date'].values() | map('string') | unique | list -%}
            {%- set paths = files.columns['source_file'].values() | list -%}
            and extraction_date in (
                {%- for day in days %}date '{{ day }}'{{ ', ' if not loop.last }}{% endfor -%}
            )
            and source_file in (
                {%- for path in paths %}'{{ path | replace("\\", "\\\\") | replace("'", "\\'") }}'{{ ', ' if not loop.last }}{% endfor -%}
            )
        {%- endif -%}
    {%- endif -%}
{%- endmacro %}

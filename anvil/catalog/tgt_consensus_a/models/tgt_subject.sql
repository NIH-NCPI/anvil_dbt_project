{{ config(schema=var('target_schema')) }}

{% set source_table = (var('source_table') | default(none)) %}

{% if source_table is not none %}
    {% do log("Using source_table: " ~ source_table, info=True) %}
    {{ transform_subject(source_table) }}
{% else %}
    {% do log("Warning source_table: " ~ source_table, info=True) %}
{% endif %}

{% macro transform_sample_external_id(source_table) %}
    select 
    sample_id::text as "Sample_id",
    external_id::text as "external_id"
    from {{ ref(source_table) }}
{%- endmacro %}
    
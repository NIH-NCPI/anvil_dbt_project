{% macro transform_sourcedata_external_id(source_table) %}
    select 
    sourcedata_id::text as "SourceData_id",
    external_id::text as "external_id"
    from {{ ref(source_table) }}
{%- endmacro %}
    
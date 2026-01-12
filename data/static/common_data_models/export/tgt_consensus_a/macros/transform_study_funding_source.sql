{% macro transform_study_funding_source(source_table) %}
    select 
    study_id::text as "Study_id",
    funding_source::text as "funding_source"
    from {{ ref(source_table) }}
{%- endmacro %}
    
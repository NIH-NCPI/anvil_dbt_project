{% macro transform_demographics_race(source_table) %}
    select 
    demographics_id::text as "Demographics_id",
    race::text as "race"
    from {{ ref(source_table) }}
{%- endmacro %}
    
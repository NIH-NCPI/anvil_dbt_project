{% macro transform_subjectassertion_external_id(source_table) %}
    select 
    subjectassertion_id::text as "SubjectAssertion_id",
    external_id::text as "external_id"
    from {{ ref(source_table) }}
{%- endmacro %}
    
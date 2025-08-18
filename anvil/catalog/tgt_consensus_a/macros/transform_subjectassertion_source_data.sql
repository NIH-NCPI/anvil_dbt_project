{% macro transform_subjectassertion_source_data(source_table) %}

select 
  subjectassertion_id::text as "SubjectAssertion_id",
  source_data_id::text as "source_data_id"
from {{ ref(source_table) }}
{%- endmacro -%}

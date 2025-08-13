{{% macro transform_participant(source_table) %}}

select 
  subjectassertion_id::text as "SubjectAssertion_id",
  external_id::text as "external_id"
from {{ ref(source_table) }}
{%- endmacro -%}

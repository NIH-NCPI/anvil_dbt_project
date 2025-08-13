{{% macro transform_participant(source_table) %}}

select 
  subjectassertion_id::text as "SubjectAssertion_id",
  source_data_id::text as "source_data_id"
from {{ ref(source_table) }}
{%- endmacro -%}

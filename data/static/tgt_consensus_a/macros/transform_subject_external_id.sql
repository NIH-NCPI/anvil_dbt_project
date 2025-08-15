{{% macro transform_participant(source_table) %}}

select 
  subject_id::text as "Subject_id",
  external_id::text as "external_id"
from {{ ref(source_table) }}
{%- endmacro -%}

{{% macro transform_participant(source_table) %}}

select 
  accesspolicy_id::text as "AccessPolicy_id",
  external_id::text as "external_id"
from {{ ref(source_table) }}
{%- endmacro -%}

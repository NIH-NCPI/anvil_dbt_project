{{% macro transform_accesspolicy_external_id(source_table) %}}

select 
  accesspolicy_id::text as "AccessPolicy_id",
  external_id::text as "external_id"
from {{ ref(source_table) }}
{%- endmacro -%}

{{% macro transform_accesspolicy_access_policy_code(source_table) %}}

select 
  accesspolicy_id::text as "AccessPolicy_id",
  access_policy_code::text as "access_policy_code"
from {{ ref(source_table) }}
{%- endmacro -%}

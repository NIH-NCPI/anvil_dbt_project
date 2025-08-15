{{% macro transform_participant(source_table) %}}

select 
  accesspolicy_id::text as "AccessPolicy_id",
  access_policy_code::text as "access_policy_code"
from {{ ref(source_table) }}
{%- endmacro -%}

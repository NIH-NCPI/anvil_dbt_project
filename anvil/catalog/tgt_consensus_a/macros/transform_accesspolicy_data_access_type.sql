{% macro transform_accesspolicy_data_access_type(source_table) %}

select 
  accesspolicy_id::text as "AccessPolicy_id",
  data_access_type::text as "data_access_type"
from {{ ref(source_table) }}
{%- endmacro -%}

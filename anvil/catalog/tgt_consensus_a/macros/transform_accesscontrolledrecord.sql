{{% macro transform_accesscontrolledrecord(source_table) %}}

select 
  has_access_policy::text as "has_access_policy",
  id::text as "id"
from {{ ref(source_table) }}
{%- endmacro -%}

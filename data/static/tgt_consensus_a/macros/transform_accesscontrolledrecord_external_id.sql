{{% macro transform_participant(source_table) %}}

select 
  accesscontrolledrecord_id::text as "AccessControlledRecord_id",
  external_id::text as "external_id"
from {{ ref(source_table) }}
{%- endmacro -%}

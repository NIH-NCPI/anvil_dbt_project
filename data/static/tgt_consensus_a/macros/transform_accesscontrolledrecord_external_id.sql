{% macro transform_accesscontrolledrecord_external_id(source_table) %}

select distinct
  accesscontrolledrecord_id::text as "AccessControlledRecord_id",
  external_id::text as "external_id"
from {{ ref(source_table) }}
{%- endmacro -%}

{% macro transform_familymember_external_id(source_table) %}

select distinct
  familymember_id::text as "FamilyMember_id",
  external_id::text as "external_id"
from {{ ref(source_table) }}
{%- endmacro -%}

{{% macro transform_participant(source_table) %}}

select 
  familymember_id::text as "FamilyMember_id",
  external_id::text as "external_id"
from {{ ref(source_table) }}
{%- endmacro -%}

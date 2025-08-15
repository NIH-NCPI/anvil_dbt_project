{{% macro transform_participant(source_table) %}}

select 
  familyrelationship_id::text as "FamilyRelationship_id",
  external_id::text as "external_id"
from {{ ref(source_table) }}
{%- endmacro -%}

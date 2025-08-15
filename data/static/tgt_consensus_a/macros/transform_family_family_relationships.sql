{{% macro transform_participant(source_table) %}}

select 
  family_id::text as "Family_id",
  family_relationships_id::text as "family_relationships_id"
from {{ ref(source_table) }}
{%- endmacro -%}

{{% macro transform_participant(source_table) %}}

select 
  family_id::text as "Family_id",
  external_id::text as "external_id"
from {{ ref(source_table) }}
{%- endmacro -%}

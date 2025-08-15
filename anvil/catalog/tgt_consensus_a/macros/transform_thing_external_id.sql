{{% macro transform_participant(source_table) %}}

select 
  thing_id::text as "Thing_id",
  external_id::text as "external_id"
from {{ ref(source_table) }}
{%- endmacro -%}

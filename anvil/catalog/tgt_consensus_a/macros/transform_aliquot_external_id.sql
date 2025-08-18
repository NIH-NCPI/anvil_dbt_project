{% macro transform_aliquot_external_id(source_table) %}

select 
  aliquot_id::text as "Aliquot_id",
  external_id::text as "external_id"
from {{ ref(source_table) }}
{%- endmacro -%}

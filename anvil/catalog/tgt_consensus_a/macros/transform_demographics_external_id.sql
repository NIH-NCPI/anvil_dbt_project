{% macro transform_demographics_external_id(source_table) %}

select 
  demographics_id::text as "Demographics_id",
  external_id::text as "external_id"
from {{ ref(source_table) }}
{%- endmacro -%}

{% macro transform_file_external_id(source_table) %}

select 
  file_id::text as "File_id",
  external_id::text as "external_id"
from {{ ref(source_table) }}
{%- endmacro -%}

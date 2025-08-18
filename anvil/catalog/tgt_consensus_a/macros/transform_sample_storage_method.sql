{% macro transform_sample_storage_method(source_table) %}

select 
  sample_id::text as "Sample_id",
  storage_method::text as "storage_method"
from {{ ref(source_table) }}
{%- endmacro -%}

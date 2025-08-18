{{% macro transform_sample_processing(source_table) %}}

select 
  sample_id::text as "Sample_id",
  processing::text as "processing"
from {{ ref(source_table) }}
{%- endmacro -%}

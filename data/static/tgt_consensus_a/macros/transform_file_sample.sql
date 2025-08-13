{{% macro transform_participant(source_table) %}}

select 
  file_id::text as "File_id",
  sample_id::text as "sample_id"
from {{ ref(source_table) }}
{%- endmacro -%}

{{% macro transform_participant(source_table) %}}

select 
  sample_id::text as "Sample_id",
  external_id::text as "external_id"
from {{ ref(source_table) }}
{%- endmacro -%}

{{% macro transform_participant(source_table) %}}

select 
  study_id::text as "Study_id",
  external_id::text as "external_id"
from {{ ref(source_table) }}
{%- endmacro -%}

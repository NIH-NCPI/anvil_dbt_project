{{% macro transform_participant(source_table) %}}

select 
  code::text as "code",
  display::text as "display",
  value_code::text as "value_code",
  value_display::text as "value_display",
  id::text as "id"
from {{ ref(source_table) }}
{%- endmacro -%}

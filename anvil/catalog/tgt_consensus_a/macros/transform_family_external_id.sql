{{% macro transform_family_external_id(source_table) %}}

select 
  family_id::text as "Family_id",
  external_id::text as "external_id"
from {{ ref(source_table) }}
{%- endmacro -%}

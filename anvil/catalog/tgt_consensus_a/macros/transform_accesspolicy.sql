{{% macro transform_accesspolicy(source_table) %}}

select 
  disease_limitation::text as "disease_limitation",
  description::text as "description",
  website::text as "website",
  id::text as "id"
from {{ ref(source_table) }}
{%- endmacro -%}

{{% macro transform_datasource_external_id(source_table) %}}

select 
  datasource_id::text as "DataSource_id",
  external_id::text as "external_id"
from {{ ref(source_table) }}
{%- endmacro -%}

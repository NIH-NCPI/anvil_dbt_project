{% macro transform_datasource_external_id(source_table) %}

select distinct
  datasource_id::text as "DataSource_id",
  external_id::text as "external_id"
from {{ ref(source_table) }}
{%- endmacro -%}

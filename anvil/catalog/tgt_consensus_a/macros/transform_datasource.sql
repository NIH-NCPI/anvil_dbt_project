{% macro transform_datasource(source_table) %}

select 
  snapshot_id::text as "snapshot_id",
  google_data_project::text as "google_data_project",
  snapshot_dataset::text as "snapshot_dataset",
  table_id::text as "table",
  parameterized_query::text as "parameterized_query",
  id::text as "id"
from {{ ref(source_table) }}
{%- endmacro -%}
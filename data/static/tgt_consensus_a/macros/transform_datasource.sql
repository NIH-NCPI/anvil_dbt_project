{% macro transform_datasource(source_table) %}

select distinct
  snapshot_id::text as "snapshot_id",
  google_data_project::text as "google_data_project",
  snapshot_dataset::text as "snapshot_dataset",
<<<<<<< HEAD
  table_id::text as "table",
=======
  table_id::text as "table_id",
>>>>>>> edbbccf2c2325febf23c942b8d5333cdedf6a335
  parameterized_query::text as "parameterized_query",
  id::text as "id"
from {{ ref(source_table) }}
{%- endmacro -%}

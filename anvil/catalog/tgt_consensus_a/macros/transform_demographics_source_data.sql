{% macro transform_demographics_source_data(source_table) %}

select distinct
  demographics_id::text as "Demographics_id",
  source_data_id::text as "source_data_id"
from {{ ref(source_table) }}
{%- endmacro -%}

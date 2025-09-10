{% macro transform_sourcedata_query_parameter(source_table) %}

select distinct
  sourcedata_id::text as "SourceData_id",
  query_parameter::text as "query_parameter"
from {{ ref(source_table) }}
{%- endmacro -%}

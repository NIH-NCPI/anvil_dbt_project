{{% macro transform_participant(source_table) %}}

select 
  sourcedata_id::text as "SourceData_id",
  query_parameter::text as "query_parameter"
from {{ ref(source_table) }}
{%- endmacro -%}

{% macro transform_sourcedata(source_table) %}

select 
  data_source::text as "data_source",
  has_access_policy::text as "has_access_policy",
  id::text as "id"
from {{ ref(source_table) }}
{%- endmacro -%}

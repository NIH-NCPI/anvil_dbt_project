{% macro transform_demographics_race(source_table) %}

select distinct
  demographics_id::text as "Demographics_id",
  race::text as "race"
from {{ ref(source_table) }}
{%- endmacro -%}

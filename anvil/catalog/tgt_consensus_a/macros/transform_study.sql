{% macro transform_study(source_table) %}

select distinct
  parent_study_id::text as "parent_study_id",
  study_title::text as "study_title",
  id::text as "id"
from {{ ref(source_table) }}
{%- endmacro -%}
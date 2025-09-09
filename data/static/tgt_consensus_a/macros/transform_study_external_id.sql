{% macro transform_study_external_id(source_table) %}

select distinct
  study_id::text as "Study_id",
  external_id::text as "external_id"
from {{ ref(source_table) }}
{%- endmacro -%}

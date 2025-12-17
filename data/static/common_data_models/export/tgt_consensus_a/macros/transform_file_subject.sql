{% macro transform_file_subject(source_table) %}

select distinct
  file_id::text as "File_id",
  subject_id::text as "subject_id"
from {{ ref(source_table) }}
{%- endmacro -%}

{{% macro transform_participant(source_table) %}}

select 
  study_id::text as "Study_id",
  principal_investigator::text as "principal_investigator"
from {{ ref(source_table) }}
{%- endmacro -%}

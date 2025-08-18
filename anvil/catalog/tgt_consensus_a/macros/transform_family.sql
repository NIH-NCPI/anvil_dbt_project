{% macro transform_family(source_table) %}

select 
  family_type::text as "family_type",
  family_description::text as "family_description",
  consanguinity::text as "consanguinity",
  family_study_focus::text as "family_study_focus",
  has_access_policy::text as "has_access_policy",
  id::text as "id"
from {{ ref(source_table) }}
{%- endmacro -%}

{% macro transform_familymember(source_table) %}

select distinct
  family_member::text as "family_member",
  family_role::text as "family_role",
  has_access_policy::text as "has_access_policy",
  id::text as "id",
  family_id::text as "Family_id"
from {{ ref(source_table) }}
{%- endmacro -%}

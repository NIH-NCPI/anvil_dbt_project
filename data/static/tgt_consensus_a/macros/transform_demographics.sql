{% macro transform_demographics(source_table) %}

select 
  date_of_birth::integer as "date_of_birth",
  date_of_birth_type::text as "date_of_birth_type",
  sex::text as "sex",
  sex_display::text as "sex_display",
  race_display::text as "race_display",
  ethnicity::text as "ethnicity",
  ethnicity_display::text as "ethnicity_display",
  age_at_last_vital_status::integer as "age_at_last_vital_status",
  vital_status::text as "vital_status",
  has_access_policy::text as "has_access_policy",
  id::text as "id"
from {{ ref(source_table) }}
{%- endmacro -%}

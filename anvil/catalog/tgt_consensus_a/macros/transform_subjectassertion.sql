{% macro transform_subjectassertion(source_table) %}

select 
  assertion_type::text as "assertion_type",
  age_at_assertion::text as "age_at_assertion",
  age_at_event::text as "age_at_event",
  age_at_resolution::text as "age_at_resolution",
  code::text as "code",
  display::text as "display",
  value_code::text as "value_code",
  value_display::text as "value_display",
  value_number::text as "value_number",
  value_units::text as "value_units",
  value_units_display::text as "value_units_display",
  has_access_policy::text as "has_access_policy",
  id::text as "id",
  subject_id::text as "Subject_id"
from {{ ref(source_table) }}
{%- endmacro -%}

{% macro transform_aliquot(source_table) %}

select distinct
  availablity_status::text as "availablity_status",
  quantity_number::text as "quantity_number",
  quantity_units::text as "quantity_units",
  concentration_number::text as "concentration_number",
  concentration_unit::text as "concentration_unit",
  has_access_policy::text as "has_access_policy",
  id::text as "id",
  sample_id::text as "Sample_id"
from {{ ref(source_table) }}
{%- endmacro -%}

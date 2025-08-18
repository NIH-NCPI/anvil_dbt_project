{% macro transform_sample(source_table) %}

select 
  parent_sample::text as "parent_sample",
  sample_type::text as "sample_type",
  availablity_status::text as "availablity_status",
  quantity_number::text as "quantity_number",
  quantity_units::text as "quantity_units",
  has_access_policy::text as "has_access_policy",
  id::text as "id",
  subject_id::text as "Subject_id",
  biospecimen_collection_id::text as "biospecimen_collection_id"
from {{ ref(source_table) }}
{%- endmacro -%}

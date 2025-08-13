{{% macro transform_participant(source_table) %}}

select 
  filename::text as "filename",
  format::text as "format",
  data_type::text as "data_type",
  size::integer as "size",
  drs_uri::text as "drs_uri",
  file_metadata::text as "file_metadata",
  has_access_policy::text as "has_access_policy",
  id::text as "id"
from {{ ref(source_table) }}
{%- endmacro -%}

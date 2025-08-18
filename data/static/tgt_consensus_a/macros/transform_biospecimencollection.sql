{{% macro transform_biospecimencollection(source_table) %}}

select 
  age_at_collection::integer as "age_at_collection",
  method::text as "method",
  site::text as "site",
  spatial_qualifier::text as "spatial_qualifier",
  laterality::text as "laterality",
  has_access_policy::text as "has_access_policy",
  id::text as "id"
from {{ ref(source_table) }}
{%- endmacro -%}

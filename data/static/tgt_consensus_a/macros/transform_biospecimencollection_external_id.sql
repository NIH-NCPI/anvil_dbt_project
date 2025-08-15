{{% macro transform_participant(source_table) %}}

select 
  biospecimencollection_id::text as "BiospecimenCollection_id",
  external_id::text as "external_id"
from {{ ref(source_table) }}
{%- endmacro -%}

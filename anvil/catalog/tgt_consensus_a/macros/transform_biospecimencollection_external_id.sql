{% macro transform_biospecimencollection_external_id(source_table) %}

select distinct
  biospecimencollection_id::text as "BiospecimenCollection_id",
  external_id::text as "external_id"
from {{ ref(source_table) }}
{%- endmacro -%}

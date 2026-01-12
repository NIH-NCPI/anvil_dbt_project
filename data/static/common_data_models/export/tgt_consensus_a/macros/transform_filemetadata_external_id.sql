{% macro transform_filemetadata_external_id(source_table) %}
    select 
    filemetadata_id::text as "FileMetadata_id",
    external_id::text as "external_id"
    from {{ ref(source_table) }}
{%- endmacro %}
    
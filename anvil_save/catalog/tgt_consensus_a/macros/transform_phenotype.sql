{% macro transform_phenotype(source_table) %}
WITH src_phenotype AS (
    SELECT
        *
    FROM {{ ref(source_table) }}
)


SELECT
    *
FROM src_phenotype c
{% endmacro %}

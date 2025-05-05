-- Requirement of dbt_pipeline/scripts/import_data.py

{% macro run_sql(sql) %}
    {% do run_query(sql) %}
{% endmacro %}

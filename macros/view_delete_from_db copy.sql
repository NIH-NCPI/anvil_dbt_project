--  dbt run-operation view_delete_from_db --args '{"fq_drop_list": ["main.participant", "main.phenotype"]}'
{% macro view_delete_from_db(fq_drop_list=[]) %}
  {% for obj in fq_drop_list %}
    {% do log("Dropping: " ~ obj, info=True) %}
    {% do adapter.execute("DROP VIEW IF EXISTS " ~ obj ~ " CASCADE") %}
  {% endfor %}
{% endmacro %}

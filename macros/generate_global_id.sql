{% macro generate_global_id(prefix='', descriptor=[], study_id='') %}
    {% set formatted_columns = [] %}
    {% for col in descriptor %}
        {% do formatted_columns.append("'" ~ study_id ~ "'") %}

        {% do formatted_columns.append("CAST(coalesce(" ~ col ~ ", '') AS TEXT)") %}
    {% endfor %}
    {{
        "'" ~ prefix ~ "' || '_' || md5(" ~
        formatted_columns | join(" || '|' || ") ~
        ")"
    }}
{% endmacro %}
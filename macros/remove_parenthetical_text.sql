{% macro remove_parenthetical_text(column_name) %}
    trim(split_part({{ column_name }}, '(', 1))
{% endmacro %}
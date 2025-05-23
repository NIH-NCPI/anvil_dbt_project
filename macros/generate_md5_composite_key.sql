{% macro generate_md5_composite_key(prefix='', columns=[]) %}
    /* 
        This macro generates the sql required to create a primary or foreign key
        by the md5 on a composite string method.

        Example usage - lacking double brackets:
            generate_md5_composite_key("p", ['id','dob'])
        generates the following sql
            'p' || '_' || md5(CAST(coalesce(id, '') AS TEXT) || '|' || CAST(coalesce(dob, '') AS TEXT))
        results in an id with the following format
            p_a32b29ffb1a85590c4a6d4cbeec18636
    */
    {% set formatted_columns = [] %}
    {% for col in columns %}
        {% do formatted_columns.append("CAST(coalesce(" ~ col ~ ", '') AS TEXT)") %}
    {% endfor %}
    {{
        "'" ~ prefix ~ "' || '_' || md5(" ~
        formatted_columns | join(" || '|' || ") ~
        ")"
    }}
{% endmacro %}
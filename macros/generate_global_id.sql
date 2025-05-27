<<<<<<< HEAD
{% macro generate_global_id(prefix='', descriptor=[], study_id='') %}
    /* 
        This macro generates the sql required to create a primary or foreign key
        by the md5 on a composite string method.

        Example usage - lacking double brackets:
           generate_md5_composite_key("p", ['id','dob'])
        generates the following sql
            'p' || '_' || md5(studyid || CAST(coalesce(id, '') AS TEXT) || '|' || CAST(coalesce(dob, '') AS TEXT))
        results in an id with the following format
            p_a32b29ffb1a85590c4a6d4cbeec18636
    */
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
=======
{% macro generate_global_id(prefix='', descriptor=[], study_id='') %}
    /* 
        This macro generates the sql required to create a primary or foreign key
        by the md5 on a composite string method.

        Example usage - lacking double brackets:
           generate_md5_composite_key("p", ['id','dob'])
        generates the following sql
            'p' || '_' || md5(studyid || CAST(coalesce(id, '') AS TEXT) || '|' || CAST(coalesce(dob, '') AS TEXT))
        results in an id with the following format
            p_a32b29ffb1a85590c4a6d4cbeec18636
    */
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
>>>>>>> b81083e (Using global id function in external_id files)
{% endmacro %}
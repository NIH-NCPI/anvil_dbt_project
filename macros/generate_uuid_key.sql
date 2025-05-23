{% macro generate_uuid_key(prefix='') %}
    /* 
        This macro generates the sql required to create a primary or foreign key
        by the uuid method, similar to the process used in locutus.

        Example usage, lacking double brackets:
            generate_uuid_key("p")
        generates the following sql
            lower(substr(encode(md5(uuid()), 'hex'), 1, 21))
        results in an id with the following format
            p-92d21b1e2e0bb57c8409b
            p-{nanoid like id: URL-safe, encoded, random hex string, limited to 21 char)
    */

    {{
        "'" ~ prefix ~ "-' || lower(" ~
        "replace(replace(replace(substr(to_base64(from_hex(md5(uuid()::text))), 1, 21), '/', '_'), '+', '-'), '=', '')" ~
        ")"
    }}
{% endmacro %}

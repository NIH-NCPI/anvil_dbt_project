

    with source as (
        select 
        NULL::text as "file_id",
        NULL::text as "external_id"
    )

    select 
        * 
    from source
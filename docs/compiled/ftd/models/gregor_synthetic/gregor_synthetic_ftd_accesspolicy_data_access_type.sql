

    with source as (
        select 
        NULL::text as "accesspolicy_id",
        NULL::text as "data_access_type"
    )

    select 
        * 
    from source
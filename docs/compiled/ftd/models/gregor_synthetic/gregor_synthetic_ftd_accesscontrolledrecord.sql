

    with source as (
        select 
       NULL::text as "has_access_policy",
       NULL::text as "id"
    )

    select 
        * 
    from source
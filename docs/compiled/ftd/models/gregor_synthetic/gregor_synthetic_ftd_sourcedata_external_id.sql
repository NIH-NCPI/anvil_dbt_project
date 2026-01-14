

    with source as (
        select 
       NULL::text as "sourcedata_id",
       NULL::text as "external_id"
    )

    select 
        * 
    from source
{{ config(materialized='table', schema='gregor_synthetic_data') }}

    with source as (
        select 
       NULL::text as "thing_id",
       NULL::text as "external_id"
    )

    select 
        * 
    from source
    
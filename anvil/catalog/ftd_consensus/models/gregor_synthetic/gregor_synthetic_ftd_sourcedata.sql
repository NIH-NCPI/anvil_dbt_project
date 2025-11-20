{{ config(materialized='table', schema='gregor_synthetic_data') }}

    with source as (
        select 
       NULL::text as "code",
       NULL::text as "display",
       NULL::text as "value_code",
       NULL::text as "value_display",
       NULL::text as "value_number",
       NULL::text as "value_units",
       NULL::text as "value_units_display",
       NULL::text as "has_access_policy",
       NULL::text as "id"
    )

    select 
        * 
    from source
    
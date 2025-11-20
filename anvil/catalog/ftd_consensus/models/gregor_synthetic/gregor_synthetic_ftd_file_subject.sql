{{ config(materialized='table', schema='gregor_synthetic_data') }}

   with source as (
        select 
       NULL::text as subject_type,
       NULL::text as "organism_type",
       NULL::text as "has_access_policy",
       NULL::text as "id",
       NULL::text as "has_demographics_id" #}
    )
    
    select 
        * 
    from source
    
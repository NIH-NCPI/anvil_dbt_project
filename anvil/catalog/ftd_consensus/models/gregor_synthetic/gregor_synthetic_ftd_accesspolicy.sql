{{ config(materialized='table', schema='gregor_synthetic_data') }}

    with source as (
        select 
        GEN_UNKNOWN.disease_limitation::text as "disease_limitation",
       GEN_UNKNOWN.description::text as "description",
       GEN_UNKNOWN.website::text as "website",
       GEN_UNKNOWN.id::text as "id"
        from {{ ref('gregor_synthetic_stg_participant') }} as participant
        join {{ ref('gregor_synthetic_stg_phenotype') }} as phenotype using (ftd_key)
    )

    select 
        * 
    from source
    
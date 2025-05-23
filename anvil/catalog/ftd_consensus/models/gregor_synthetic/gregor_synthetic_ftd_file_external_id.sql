{{ config(materialized='table', schema='gregor_synthetic_data') }}

    with source as (
        select 
        GEN_UNKNOWN.File_id::text as "File_id",
       GEN_UNKNOWN.external_id::text as "external_id"
        from {{ ref('gregor_synthetic_stg_participant') }} as participant
        join {{ ref('gregor_synthetic_stg_phenotype') }} as phenotype using (ftd_key)
    )

    select 
        * 
    from source
    
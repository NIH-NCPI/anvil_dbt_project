{{ config(materialized='table', schema='gregor_synthetic_data') }}

    with source as (
        select 
        GEN_UNKNOWN.family_member::text as "family_member",
       GEN_UNKNOWN.family_role::text as "family_role",
       GEN_UNKNOWN.has_access_policy::text as "has_access_policy",
       GEN_UNKNOWN.id::text as "id",
       GEN_UNKNOWN.Family_id::text as "Family_id"
        from {{ ref('gregor_synthetic_stg_participant') }} as participant
        join {{ ref('gregor_synthetic_stg_phenotype') }} as phenotype using (ftd_key)
    )

    select 
        * 
    from source
    
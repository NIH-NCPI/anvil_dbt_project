{{ config(materialized='table', schema='gregor_synthetic_data') }}

    with source as (
        select 
        GEN_UNKNOWN.family_member::text as "family_member",
       GEN_UNKNOWN.other_family_member::text as "other_family_member",
       GEN_UNKNOWN.relationship_code::text as "relationship_code",
       GEN_UNKNOWN.has_access_policy::text as "has_access_policy",
       GEN_UNKNOWN.id::text as "id"
        from {{ ref('gregor_synthetic_stg_participant') }} as participant
        join {{ ref('gregor_synthetic_stg_phenotype') }} as phenotype using (ftd_key)
    )

    select 
        * 
    from source
    
{{ config(materialized='table', schema='gregor_synthetic_data') }}

    with source as (
        select 
        -- GEN_UNKNOWN.family_member::text as "family_member",
    --    GEN_UNKNOWN.other_family_member::text as "other_family_member",
       participant.relationship_code::text as "relationship_code",
    --    GEN_UNKNOWN.has_access_policy::text as "has_access_policy",
        {{ generate_global_id(prefix='fr',descriptor=['participant.AnVIL_GREGoR_GSS_U07_GRU_participant_id'], study_id='gregor_synthetic') }}::text as "id",
        from {{ ref('gregor_synthetic_stg_participant') }} as participant
        -- join {{ ref('gregor_synthetic_stg_phenotype') }} as phenotype using (ftd_key)
    )

    select 
        * 
    from source
    
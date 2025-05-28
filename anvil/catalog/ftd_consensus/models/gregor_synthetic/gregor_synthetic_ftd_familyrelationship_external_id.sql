{{ config(materialized='table', schema='gregor_synthetic_data') }}

    with source as (
        select 
        {{ generate_global_id(prefix='fr',descriptor=['participant.AnVIL_GREGoR_GSS_U07_GRU_participant_id'], study_id='gregor_synthetic') }}::text as "FamilyRelationship_id",
        participant.AnVIL_GREGoR_GSS_U07_GRU_participant_id::text as "external_id"
        from {{ ref('gregor_synthetic_stg_participant') }} as participant
        -- join {{ ref('gregor_synthetic_stg_phenotype') }} as phenotype using (ftd_key)
    )

    select 
        * 
    from source
    
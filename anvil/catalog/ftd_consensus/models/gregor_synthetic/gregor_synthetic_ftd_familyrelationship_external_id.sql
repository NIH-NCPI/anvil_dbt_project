{{ config(materialized='table', schema='gregor_synthetic_data') }}

    with source as (
        select 
<<<<<<< HEAD
        {{ generate_global_id(prefix='',descriptor=[''], study_id='gregor_synthetic') }}::text as "familyrelationship_id",
=======
        {{ generate_global_id(prefix='fr',descriptor=['participant.AnVIL_GREGoR_GSS_U07_GRU_participant_id'], study_id='gregor_synthetic') }}::text as "FamilyRelationship_id",
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b81083e (Using global id function in external_id files)
       GEN_UNKNOWN.external_id::text as "external_id"
=======
    --    GEN_UNKNOWN.external_id::text as "external_id"
>>>>>>> f1f1cfe (Modified family relationship file, external_id files)
        from {{ ref('gregor_synthetic_stg_participant') }} as participant
        join {{ ref('gregor_synthetic_stg_phenotype') }} as phenotype
on participant.anvil_gregor_gss_u07_gru_participant_id = phenotype.participant_id 
=======
        participant.AnVIL_GREGoR_GSS_U07_GRU_participant_id::text as "external_id"
        from {{ ref('gregor_synthetic_stg_participant') }} as participant
        -- join {{ ref('gregor_synthetic_stg_phenotype') }} as phenotype using (ftd_key)
>>>>>>> 9cff46c (Modified external id's in external id files)
    )

    select 
        * 
    from source
    
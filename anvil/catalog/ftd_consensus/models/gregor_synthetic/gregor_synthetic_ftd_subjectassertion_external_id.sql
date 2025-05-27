{{ config(materialized='table', schema='gregor_synthetic_data') }}

    with source as (
        select 
<<<<<<< HEAD
        {{ generate_global_id(prefix='sa',descriptor=['participant.anvil_gregor_gss_u07_gru_participant_id'], study_id='gregor_synthetic') }}::text as "SubjectAssertion_id",
       GEN_UNKNOWN.external_id::text as "external_id"
        from {{ ref('gregor_synthetic_stg_participant') }} as participant
        join {{ ref('gregor_synthetic_stg_phenotype') }} as phenotype
on participant.anvil_gregor_gss_u07_gru_participant_id = phenotype.participant_id 
=======
        {{ generate_global_id(prefix='sa',descriptor=['phenotype.AnVIL_GREGoR_GSS_U07_GRU_phenotype_id'], study_id='gregor_synthetic') }}::text as "SubjectAssertion_id",
        -- GEN_UNKNOWN.external_id::text as "external_id"
        from {{ ref('gregor_synthetic_stg_participant') }} as participant
        join {{ ref('gregor_synthetic_stg_phenotype') }} as phenotype
        on participant.AnVIL_GREGoR_GSS_U07_GRU_participant_id = phenotype.participant_id 
>>>>>>> b81083e (Using global id function in external_id files)
    )

    select 
        * 
    from source
    
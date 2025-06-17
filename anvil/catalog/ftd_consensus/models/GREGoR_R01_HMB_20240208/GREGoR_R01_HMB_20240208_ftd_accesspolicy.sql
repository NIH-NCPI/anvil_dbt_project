{{ config(materialized='table', schema='GREGoR_R01_HMB_20240208_data') }}

    with source as (
        select 
    --     GEN_UNKNOWN.disease_limitation::text as "disease_limitation",
    --    GEN_UNKNOWN.description::text as "description",
    --    GEN_UNKNOWN.website::text as "website",
      {{ generate_global_id(prefix='ap',descriptor=['participant.consent_code'], study_id='GREGoR_R01_HMB_20240208') }}::text as "id"
        from {{ ref('GREGoR_R01_HMB_20240208_stg_participant') }} as participant
        join {{ ref('GREGoR_R01_HMB_20240208_stg_phenotype') }} as phenotype
on participant.participant_id = phenotype.participant_id 
    )

    select 
        * 
    from source
    
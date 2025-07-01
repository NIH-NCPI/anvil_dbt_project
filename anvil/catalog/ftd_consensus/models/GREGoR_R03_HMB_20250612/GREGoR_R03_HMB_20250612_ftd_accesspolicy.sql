{{ config(materialized='table', schema='GREGoR_R03_HMB_20250612_data') }}

    with source as (
        select 
    --     GEN_UNKNOWN.disease_limitation::text as "disease_limitation",
    --    GEN_UNKNOWN.description::text as "description",
    --    GEN_UNKNOWN.website::text as "website",
      {{ generate_global_id(prefix='ap',descriptor=['participant.consent_code'], study_id='GREGoR_R03_HMB_20250612') }}::text as "id"
        from {{ ref('GREGoR_R03_HMB_20250612_stg_participant') }} as participant
        join {{ ref('GREGoR_R03_HMB_20250612_stg_phenotype') }} as phenotype
on participant.participant_id = phenotype.participant_id 
    )

    select 
        * 
    from source
    
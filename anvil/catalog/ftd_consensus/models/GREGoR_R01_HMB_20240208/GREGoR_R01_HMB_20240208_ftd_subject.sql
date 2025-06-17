{{ config(materialized='table', schema='GREGoR_R01_HMB_20240208_data') }}
    with source as (
        select 
        -- GEN_UNKNOWN.subject_type::text as "subject_type",
    --    GEN_UNKNOWN.organism_type::text as "organism_type",
       {{ generate_global_id(prefix='ap',descriptor=['participant.consent_code'], study_id='GREGoR_R01_HMB_20240208') }}::text as "has_access_policy",
       {{ generate_global_id(prefix='sb',descriptor=['participant.participant_id'], study_id='GREGoR_R01_HMB_20240208') }}::text as "id",
    {# {{ generate_global_id(prefix='',descriptor=[''], study_id='GREGoR_R01_HMB_20240208') }}::text as "has_demographics_id" #}
        from {{ ref('GREGoR_R01_HMB_20240208_stg_participant') }} as participant
    )

    select 
        * 
    from source
    
{{ config(materialized='table', schema='GREGoR_R03_HMB_20250612_data') }}
    with source as (
        select 
       CASE 
            WHEN experiment.participant_id IS NOT NULL then 'participant'
            ELSE 'non_participant'
        END::text as subject_type,
        --    GEN_UNKNOWN.organism_type::text as "organism_type",
       {{ generate_global_id(prefix='ap',descriptor=['participant.consent_code'], study_id='phs003047') }}::text as "has_access_policy",
       {{ generate_global_id(prefix='sb',descriptor=['participant.participant_id'], study_id='phs003047') }}::text as "id",
    {# {{ generate_global_id(prefix='',descriptor=[''], study_id='GREGoR_R03_HMB_20250612') }}::text as "has_demographics_id" #}
        from {{ ref('GREGoR_R03_HMB_20250612_stg_participant') }} as participant
        join {{ ref('GREGoR_R03_HMB_20250612_stg_experiment') }} as experiment
        on participant.participant_id = experiment.participant_id 
    )

    select 
        * 
    from source
    
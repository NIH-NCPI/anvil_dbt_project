{{ config(materialized='table', schema='gregor_synthetic_data') }}
    with source as (
        select 
        -- GEN_UNKNOWN.subject_type::text as "subject_type",
    --    GEN_UNKNOWN.organism_type::text as "organism_type",
       {{ generate_global_id(prefix='ap',descriptor=['participant.consent_code'], study_id='gregor_synthetic') }}::text as "has_access_policy",
       {{ generate_global_id(prefix='sb',descriptor=['participant.anvil_gregor_gss_u07_gru_participant_id'], study_id='gregor_synthetic') }}::text as "id",
    {# {{ generate_global_id(prefix='',descriptor=[''], study_id='gregor_synthetic') }}::text as "has_demographics_id" #}
        from {{ ref('gregor_synthetic_stg_participant') }} as participant
    )

    select 
        * 
    from source
    
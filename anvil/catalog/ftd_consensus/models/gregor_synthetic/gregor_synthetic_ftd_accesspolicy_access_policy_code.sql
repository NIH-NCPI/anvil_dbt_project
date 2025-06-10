{{ config(materialized='table', schema='gregor_synthetic_data') }}

    with source as (
        select 
        {{ generate_global_id(prefix='ap',descriptor=['participant.consent_code'], study_id='gregor_synthetic') }}::text as "accesspolicy_id",
        participant.consent_code::text as "access_policy_code"
        from {{ ref('gregor_synthetic_stg_participant') }} as participant
        join {{ ref('gregor_synthetic_stg_phenotype') }} as phenotype
on participant.anvil_gregor_gss_u07_gru_participant_id = phenotype.participant_id 
    )

    select 
        * 
    from source
    
{{ config(materialized='table', schema='gregor_synthetic_data') }}

    with source as (
        select 
        {{ generate_global_id(prefix='sa',descriptor=['participant.anvil_gregor_gss_u07_gru_participant_id'], study_id='gregor_synthetic') }}::text as "SubjectAssertion_id",
       participant.anvil_gregor_gss_u07_gru_participant_id::text as "external_id"
        from {{ ref('gregor_synthetic_stg_participant') }} as participant
        join {{ ref('gregor_synthetic_stg_phenotype') }} as phenotype
on participant.anvil_gregor_gss_u07_gru_participant_id = phenotype.participant_id 
    )

    select 
        * 
    from source
    
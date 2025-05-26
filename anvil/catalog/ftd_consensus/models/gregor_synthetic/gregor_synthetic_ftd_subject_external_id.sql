{{ config(materialized='table', schema='gregor_synthetic_data') }}

    with source as (
        select 
        {{ generate_global_id(prefix='',descriptor=[''], study_id='gregor_synthetic') }}::text as "subject_id",
       GEN_UNKNOWN.external_id::text as "external_id"
        from {{ ref('gregor_synthetic_stg_participant') }} as participant
        join {{ ref('gregor_synthetic_stg_phenotype') }} as phenotype
on participant.anvil_gregor_gss_u07_gru_participant_id = phenotype.participant_id 
    )

    select 
        * 
    from source
    
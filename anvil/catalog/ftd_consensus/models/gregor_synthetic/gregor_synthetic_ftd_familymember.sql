{{ config(materialized='table', schema='gregor_synthetic_data') }}

    with source as (
        select 
        {{ generate_global_id(prefix='',descriptor=[''], study_id='gregor_synthetic') }}::text as "family_member",
       GEN_UNKNOWN.family_role::text as "family_role",
       {{ generate_global_id(prefix='',descriptor=[''], study_id='gregor_synthetic') }}::text as "has_access_policy",
       {{ generate_global_id(prefix='',descriptor=[''], study_id='gregor_synthetic') }}::text as "id",
       {{ generate_global_id(prefix='',descriptor=[''], study_id='gregor_synthetic') }}::text as "family_id"
        from {{ ref('gregor_synthetic_stg_participant') }} as participant
        join {{ ref('gregor_synthetic_stg_phenotype') }} as phenotype
on participant.anvil_gregor_gss_u07_gru_participant_id = phenotype.participant_id 
    )

    select 
        * 
    from source
    
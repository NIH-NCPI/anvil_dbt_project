{{ config(materialized='table', schema='gregor_synthetic_data') }}

    with source as (
        select 
        GEN_UNKNOWN.disease_limitation::text as "disease_limitation",
       GEN_UNKNOWN.description::text as "description",
       GEN_UNKNOWN.website::text as "website",
       {{ generate_global_id(prefix='',descriptor=[''], study_id='gregor_synthetic') }}::text as "id"
        from {{ ref('gregor_synthetic_stg_participant') }} as participant
        join {{ ref('gregor_synthetic_stg_phenotype') }} as phenotype
on participant.anvil_gregor_gss_u07_gru_participant_id = phenotype.participant_id 
    )

    select 
        * 
    from source
    
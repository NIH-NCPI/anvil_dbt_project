{{ config(materialized='table', schema='gregor_synthetic_data') }}

    with source as (
        select 
        GEN_UNKNOWN.family_type::text as "family_type",
       GEN_UNKNOWN.family_description::text as "family_description",
       GEN_UNKNOWN.consanguinity::text as "consanguinity",
       GEN_UNKNOWN.family_study_focus::text as "family_study_focus",
       {{ generate_global_id(prefix='',descriptor=[''], study_id='gregor_synthetic') }}::text as "has_access_policy",
        -- participant.family_id::text as "id"
       -- Check if using the auto-generated id is correct. 
       {{ generate_global_id(prefix='',descriptor=[''], study_id='gregor_synthetic') }}::text as "id"
        from {{ ref('gregor_synthetic_stg_participant') }} as participant
        join {{ ref('gregor_synthetic_stg_phenotype') }} as phenotype
        on participant.participant_id = phenotype.participant_id 
    )

    select 
        * 
    from source
    
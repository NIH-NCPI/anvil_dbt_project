{{ config(materialized='table', schema='gregor_synthetic_data') }}

    with source as (
        select 
       {{ generate_global_id(prefix='sb',descriptor=['participant.participant_id'], study_id='gregor_synthetic') }}::text as "Subject_id",
       participant.participant_id::text as "external_id"
        from {{ ref('gregor_synthetic_stg_participant') }} as participant
        join {{ ref('gregor_synthetic_stg_phenotype') }} as phenotype
on participant.participant_id = phenotype.participant_id 
    )

    select 
        * 
    from source
    
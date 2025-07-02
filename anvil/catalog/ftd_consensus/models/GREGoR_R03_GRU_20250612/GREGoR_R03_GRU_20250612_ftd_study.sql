{{ config(materialized='table', schema='GREGoR_R03_GRU_20250612_data') }}

    with source as (
        select 
        {{ generate_global_id(prefix='',descriptor=[''], study_id='GREGoR_R03_GRU_20250612') }}::text as "parent_study_id",
       GEN_UNKNOWN.study_title::text as "study_title",
       {{ generate_global_id(prefix='',descriptor=[''], study_id='GREGoR_R03_GRU_20250612') }}::text as "id"
        from {{ ref('GREGoR_R03_GRU_20250612_stg_participant') }} as participant
        join {{ ref('GREGoR_R03_GRU_20250612_stg_phenotype') }} as phenotype
on participant.participant_id = phenotype.participant_id 
    )

    select 
        * 
    from source
    
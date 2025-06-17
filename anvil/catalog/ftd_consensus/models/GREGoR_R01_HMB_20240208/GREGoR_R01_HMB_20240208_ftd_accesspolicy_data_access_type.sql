{{ config(materialized='table', schema='GREGoR_R01_HMB_20240208_data') }}

    with source as (
        select 
        {{ generate_global_id(prefix='',descriptor=[''], study_id='GREGoR_R01_HMB_20240208') }}::text as "accesspolicy_id",
       GEN_UNKNOWN.data_access_type::text as "data_access_type"
        from {{ ref('GREGoR_R01_HMB_20240208_stg_participant') }} as participant
        join {{ ref('GREGoR_R01_HMB_20240208_stg_phenotype') }} as phenotype
on participant.participant_id = phenotype.participant_id 
    )

    select 
        * 
    from source
    
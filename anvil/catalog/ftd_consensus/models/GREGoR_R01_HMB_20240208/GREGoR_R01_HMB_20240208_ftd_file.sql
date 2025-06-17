{{ config(materialized='table', schema='GREGoR_R01_HMB_20240208_data') }}

    with source as (
        select 
        GEN_UNKNOWN.filename::text as "filename",
       GEN_UNKNOWN.format::text as "format",
       GEN_UNKNOWN.data_type::text as "data_type",
       GEN_UNKNOWN.size::integer as "size",
       GEN_UNKNOWN.drs_uri::text as "drs_uri",
       {{ generate_global_id(prefix='',descriptor=[''], study_id='GREGoR_R01_HMB_20240208') }}::text as "file_metadata",
       {{ generate_global_id(prefix='',descriptor=[''], study_id='GREGoR_R01_HMB_20240208') }}::text as "has_access_policy",
       {{ generate_global_id(prefix='',descriptor=[''], study_id='GREGoR_R01_HMB_20240208') }}::text as "id"
        from {{ ref('GREGoR_R01_HMB_20240208_stg_participant') }} as participant
        join {{ ref('GREGoR_R01_HMB_20240208_stg_phenotype') }} as phenotype
on participant.participant_id = phenotype.participant_id 
    )

    select 
        * 
    from source
    
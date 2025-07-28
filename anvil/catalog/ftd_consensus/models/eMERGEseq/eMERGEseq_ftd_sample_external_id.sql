{{ config(materialized='table', schema='eMERGEseq_data') }}

    with source as (
        select 
        {{ generate_global_id(prefix='sm',descriptor=['sample_id'], study_id='eMERGEseq') }}::text as "sample_id",
--        GEN_UNKNOWN.external_id::text as "external_id"
        from {{ ref('eMERGEseq_stg_sampleattributes') }} as sampleattributes
    )

    select 
        * 
    from source
    
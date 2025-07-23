{{ config(materialized='table', schema='cmg_bh_data') }}

    with source as (
        select 
        GEN_UNKNOWN.filename::text as "filename",
       GEN_UNKNOWN.format::text as "format",
       GEN_UNKNOWN.data_type::text as "data_type",
       GEN_UNKNOWN.size::integer as "size",
       GEN_UNKNOWN.drs_uri::text as "drs_uri",
       {{ generate_global_id(prefix='',descriptor=[''], study_id='cmg_bh') }}::text as "file_metadata",
       {{ generate_global_id(prefix='',descriptor=[''], study_id='cmg_bh') }}::text as "has_access_policy",
       {{ generate_global_id(prefix='',descriptor=[''], study_id='cmg_bh') }}::text as "id"
        from {{ ref('cmg_bh_stg_sample') }} as sample
        join {{ ref('cmg_bh_stg_subject') }} as subject
on sample.subject_id = subject.subject_id 
    )

    select 
        * 
    from source
    
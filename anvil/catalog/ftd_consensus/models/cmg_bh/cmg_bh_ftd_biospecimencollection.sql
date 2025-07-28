{{ config(materialized='table', schema='cmg_bh_data') }}

    with source as (
        select 
        GEN_UNKNOWN.age_at_collection::integer as "age_at_collection",
       GEN_UNKNOWN.method::text as "method",
       GEN_UNKNOWN.site::text as "site",
       GEN_UNKNOWN.spatial_qualifier::text as "spatial_qualifier",
       GEN_UNKNOWN.laterality::text as "laterality",
       {{ generate_global_id(prefix='',descriptor=[''], study_id='cmg_bh') }}::text as "has_access_policy",
       {{ generate_global_id(prefix='',descriptor=[''], study_id='cmg_bh') }}::text as "id"
        from {{ ref('cmg_bh_stg_sample') }} as sample
        join {{ ref('cmg_bh_stg_subject') }} as subject
on sample.subject_id = subject.subject_id 
    )

    select 
        * 
    from source
    
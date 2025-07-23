{{ config(materialized='table', schema='cmg_bh_data') }}

    with source as (
        select 
        GEN_UNKNOWN.family_type::text as "family_type",
       GEN_UNKNOWN.family_description::text as "family_description",
       GEN_UNKNOWN.consanguinity::text as "consanguinity",
       GEN_UNKNOWN.family_study_focus::text as "family_study_focus",
       {{ generate_global_id(prefix='',descriptor=[''], study_id='cmg_bh') }}::text as "has_access_policy",
       {{ generate_global_id(prefix='',descriptor=[''], study_id='cmg_bh') }}::text as "id"
        from {{ ref('cmg_bh_stg_sample') }} as sample
        join {{ ref('cmg_bh_stg_subject') }} as subject
on sample.subject_id = subject.subject_id 
    )

    select 
        * 
    from source
    
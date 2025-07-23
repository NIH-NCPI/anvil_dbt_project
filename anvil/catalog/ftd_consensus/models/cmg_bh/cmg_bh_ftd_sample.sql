{{ config(materialized='table', schema='cmg_bh_data') }}

    with source as (
        select 
        {{ generate_global_id(prefix='',descriptor=[''], study_id='cmg_bh') }}::text as "parent_sample",
       GEN_UNKNOWN.sample_type::text as "sample_type",
       GEN_UNKNOWN.availablity_status::text as "availablity_status",
       GEN_UNKNOWN.quantity_number::text as "quantity_number",
       GEN_UNKNOWN.quantity_units::text as "quantity_units",
       {{ generate_global_id(prefix='',descriptor=[''], study_id='cmg_bh') }}::text as "has_access_policy",
       {{ generate_global_id(prefix='',descriptor=[''], study_id='cmg_bh') }}::text as "id",
       {{ generate_global_id(prefix='',descriptor=[''], study_id='cmg_bh') }}::text as "subject_id",
       {{ generate_global_id(prefix='',descriptor=[''], study_id='cmg_bh') }}::text as "biospecimen_collection_id"
        from {{ ref('cmg_bh_stg_sample') }} as sample
        join {{ ref('cmg_bh_stg_subject') }} as subject
on sample.subject_id = subject.subject_id 
    )

    select 
        * 
    from source
    
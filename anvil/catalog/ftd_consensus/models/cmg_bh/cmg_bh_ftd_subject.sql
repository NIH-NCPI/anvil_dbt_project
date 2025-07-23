{{ config(materialized='table', schema='cmg_bh_data') }}

    with source as (
        select 
       'participant'::text as "subject_type",
       'human'::text as "organism_type",
       {{ generate_global_id(prefix='ap',descriptor=['subject.subject_id','subject.ingest_provenance'], study_id='cmg_bh') }}::text as "has_access_policy",
       {{ generate_global_id(prefix='sb',descriptor=['subject.subject_id'], study_id='cmg_bh') }}::text as "id",
       {{ generate_global_id(prefix='dm',descriptor=['subject.subject_id'], study_id='cmg_bh') }}::text as "has_demographics_id"
        from {{ ref('cmg_bh_stg_subject') }} as subject
    )

    select 
     source.subject_type,
     source.organism_type,
     source.has_access_policy,  -- TODO
     source.id,-- TODO
     source.has_demographics_id -- TODO
    from source
    
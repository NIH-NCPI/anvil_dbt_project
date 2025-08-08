{{ config(materialized='table', schema='cmg_bh_data') }}
--testing out the tgt model src_data_id moved to sa_external_id.sql
-- select 
--   { { generate_global_id(prefix='sa',descriptor=['subject_id','code'], study_id='cmg_bh') }}::text as "subjectassertion_id",
--   { { generate_global_id(prefix='sd',descriptor=['dbgap_study_id'], study_id='cmg_bh') }}::text as "source_data_id"
-- from {{ ref('cmg_bh_stg_sample') }} as sample
-- join {{ ref('cmg_bh_stg_subject') }} as subject
-- on sample.subject_id = subject.subject_id 
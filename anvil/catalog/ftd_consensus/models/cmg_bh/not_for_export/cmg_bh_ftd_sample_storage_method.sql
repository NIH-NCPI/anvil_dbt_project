{{ config(materialized='table', schema='cmg_bh_data') }}

select 
  {{ generate_global_id(prefix='sm',descriptor=['sample_id','subject_id'], study_id='cmg_bh') }}::text as "sample_id",
  NULL::text as "storage_method"
from (select distinct sample_id, subject_id from {{ ref('cmg_bh_stg_sample') }}
     left join
     select distinct ingest_provenance, subject_id from {{ ref('cmg_bh_stg_subject') }}
     using (subject_id)
     ) as s
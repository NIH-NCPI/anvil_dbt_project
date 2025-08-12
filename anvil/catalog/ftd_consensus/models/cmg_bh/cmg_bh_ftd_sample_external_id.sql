{{ config(materialized='table', schema='cmg_bh_data') }}

select 
  distinct
  {{ generate_global_id(prefix='sm',descriptor=['sample_id','subject_id'], study_id='cmg_bh') }}::text as "sample_id",
  sample_id::text as "external_id"
from (select distinct sample_id, subject_id, ingest_provenance
    from (select distinct subject_id, ingest_provenance, sample_id from {{ ref('cmg_bh_stg_sample') }}) as sam
    join (select distinct subject_id, ingest_provenance from {{ ref('cmg_bh_stg_subject') }}) as sub
    using (subject_id, ingest_provenance)
      )
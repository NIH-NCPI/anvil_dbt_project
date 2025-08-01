{{ config(materialized='table', schema='cmg_bh_data') }}

with 
sam_slice as (
    select
      sample_id, ingest_provenance
    from (select distinct subject_id, sample_id from {{ ref('cmg_bh_stg_sample') }}) as sam
    join (select distinct subject_id, ingest_provenance from {{ ref('cmg_bh_stg_subject') }}) as sub
    using (subject_id)
)

select 
  {{ generate_global_id(prefix='ap',descriptor=['ingest_provenance'], study_id='cmg_bh') }}::text as "has_access_policy",
  {{ generate_global_id(prefix='bc',descriptor=['sample_id'], study_id='cmg_bh') }}::text as "id"
from sam_slice

    
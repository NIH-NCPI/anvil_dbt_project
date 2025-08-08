{{ config(materialized='table', schema='cmg_bh_data') }}

select 
  NULL::integer as "age_at_collection",
  NULL::text as "method",
  sample_source::text as "site",
  NULL::text as "spatial_qualifier",
  NULL::text as "laterality",
  {{ generate_global_id(prefix='ap',descriptor=['ingest_provenance'], study_id='cmg_bh') }}::text as "has_access_policy",
  {{ generate_global_id(prefix='bc',descriptor=['sample_id','sample_source'], study_id='cmg_bh') }}::text as "id"
from (select distinct sample_id, sample_source, ingest_provenance
    from (select distinct subject_id, sample_id from {{ ref('cmg_bh_stg_sample') }}) as sam
    join (select distinct subject_id, ingest_provenance from {{ ref('cmg_bh_stg_subject') }}) as sub
    using (subject_id)
      )
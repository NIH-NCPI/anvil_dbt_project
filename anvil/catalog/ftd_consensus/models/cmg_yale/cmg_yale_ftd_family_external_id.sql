{{ config(materialized='table', schema='cmg_yale_data') }}

select 
  {{ generate_global_id(prefix='fy',descriptor=['family_id'], study_id='cmg_bh') }}::text as "family_id"
   family_id::text as "external_id"
from (select 
      distinct subject_id, ingest_provenance, family_id 
      from {{ ref('cmg_yale_stg_subject') }}
    left join
      select 
      distinct consanguinity, family_id 
      from {{ ref('cmg_yale_stg_family') }}
    using (ingest_provenance, family_id)
      ) as s 
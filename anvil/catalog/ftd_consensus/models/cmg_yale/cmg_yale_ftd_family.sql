{{ config(materialized='table', schema='cmg_yale_data') }} 

select 
  NULL::text as "family_type",
  NULL::text as "family_description",
  consanguinity::text as "consanguinity",
  NULL::text as "family_study_focus",
  {{ generate_global_id(prefix='ap',descriptor=['ingest_provenance'], study_id='cmg_bh') }}::text as "has_access_policy",
  {{ generate_global_id(prefix='fy',descriptor=['family_id'], study_id='cmg_bh') }}::text as "id"
from (select 
      distinct subject_id, ingest_provenance, family_id 
      from {{ ref('cmg_yale_stg_subject') }}
    left join
      select 
      distinct consanguinity, family_id 
      from {{ ref('cmg_yale_stg_family') }}
    using (ingest_provenance, family_id)
      ) as s 
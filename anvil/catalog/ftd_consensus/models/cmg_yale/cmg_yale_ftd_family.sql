{{ config(materialized='table', schema='cmg_yale_data') }} 

select 
  NULL::text as "family_type",
  NULL::text as "family_description",
  consanguinity::text as "consanguinity",
  NULL::text as "family_study_focus",
  {{ generate_global_id(prefix='ap',descriptor=['consent_id'], study_id='cmg_yale') }}::text as "has_access_policy",
  {{ generate_global_id(prefix='fy',descriptor=['family_id'], study_id='cmg_yale') }}::text as "id"
from (select 
        distinct consent_id, family_id, consanguinity
      from {{ ref('cmg_yale_stg_family') }}
      ) as s 
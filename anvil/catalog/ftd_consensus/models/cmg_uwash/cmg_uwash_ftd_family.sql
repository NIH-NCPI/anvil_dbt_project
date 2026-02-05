{{ config(materialized='table', schema='cmg_uwash_data') }}

select 
  NULL::text as "family_type",
  NULL::text as "family_description",
  
  code::text as "consanguinity",
  
  NULL::text as "family_study_focus",
  {{ generate_global_id(prefix='ap',descriptor=['consent_id'], study_id='phs000693') }}::text as "has_access_policy",
  {{ generate_global_id(prefix='fy',descriptor=['family_id'], study_id='phs000693') }}::text as "id"
from (select 
      distinct consent_id, family_id, code, consanguinity
      from {{ ref('cmg_uwash_stg_family') }} as fam
      left join {{ ref('fm_consanguinity') }} as fc
      on lower(fam.consanguinity) = fc.src_format
      ) as s 
{{ config(materialized='table', schema='cmg_uwash_data') }}

select 
  distinct
  coalesce(curie, 'FAMMEMB')::text as "family_role",
  {{ generate_global_id(prefix='sb',descriptor=['subject_id', 'consent_id'], study_id='phs000693') }}::text as "family_member",
  {{ generate_global_id(prefix='ap',descriptor=['consent_id'], study_id='phs000693') }}::text as "has_access_policy",
  {{ generate_global_id(prefix='fm',descriptor=['family_id','subject_id', 'consent_id'], study_id='phs000693') }}::text as "id",
  {{ generate_global_id(prefix='fy',descriptor=['family_id'], study_id='phs000693') }}::text as "family_id"
from (select distinct subject_id, consent_id, family_id, curie, relationship_to_proband, proband_relationship
      from {{ ref('cmg_uwash_stg_subject') }} as sub
      left join {{ ref('fm_family_role') }} as role
      on lower(sub.proband_relationship) = lower(role.relationship_to_proband)
      ) as s

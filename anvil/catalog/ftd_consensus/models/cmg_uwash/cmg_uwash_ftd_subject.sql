{{ config(materialized='table', schema='cmg_uwash_data') }}

select 
'participant'::text as "subject_type",
  'human'::text as "organism_type",
    {{ generate_global_id(prefix='ap',descriptor=['consent_id'], study_id='phs000693') }}::text as "has_access_policy",
    {{ generate_global_id(prefix='sb',descriptor=['subject_id','consent_id'], study_id='phs000693') }}::text as "id",
    {{ generate_global_id(prefix='dm',descriptor=['subject_id','consent_id'], study_id='phs000693') }}::text as "has_demographics_id"
from {{ ref('cmg_uwash_stg_subject') }} as subject
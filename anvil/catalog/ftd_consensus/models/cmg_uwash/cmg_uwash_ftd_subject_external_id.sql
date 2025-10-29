{{ config(materialized='table', schema='cmg_uwash_data') }}

select distinct
  {{ generate_global_id(prefix='sb',descriptor=['subject_id', 'consent_id'], study_id='phs000693') }}::text as "subject_id",
  subject_id::text as "external_id"
from {{ ref('cmg_uwash_stg_subject') }} as subject
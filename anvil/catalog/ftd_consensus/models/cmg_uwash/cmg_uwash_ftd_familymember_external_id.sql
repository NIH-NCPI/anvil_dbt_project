{{ config(materialized='table', schema='cmg_uwash_data') }}

select 
  {{ generate_global_id(prefix='fm',descriptor=['family_id','subject_id'], study_id='phs000693') }}::text as "familymember_id",
  subject_id::text as "external_id"
from (select distinct subject_id, family_id from {{ ref('cmg_uwash_stg_subject') }}) as subject


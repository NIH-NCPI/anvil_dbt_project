{{ config(materialized='table', schema='cmg_uwash_data') }}

select 
  {{ generate_global_id(prefix='sm',descriptor=['subject_id','sample_id'], study_id='phs000693') }}::text as "sample_id",
  sample_id::text as "external_id"
from (select distinct subject_id, sample_id from {{ ref('cmg_uwash_stg_sample') }}) as sample

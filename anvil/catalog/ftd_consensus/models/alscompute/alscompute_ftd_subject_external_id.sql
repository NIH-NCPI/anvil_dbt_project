{{ config(materialized='table', schema='alscompute_data') }}

select 
  {{ generate_global_id(prefix='sb',descriptor=['sample_id','consent_id'], study_id='alscompute') }}::text as "subject_id",
  sample_id::text as "external_id"
from (select distinct sample_id, consent_id from {{ ref('alscompute_stg_sample') }}) as sample

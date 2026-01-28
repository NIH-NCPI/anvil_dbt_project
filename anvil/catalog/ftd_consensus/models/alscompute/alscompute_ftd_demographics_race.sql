{{ config(materialized='table', schema='alscompute_data') }}

select 
  {{ generate_global_id(prefix='dm',descriptor=['sample_id','consent_id'], study_id='alscompute') }}::text as "demographics_id",
  'unknown'::text as "race"
from (select distinct sample_id, consent_id from {{ ref('alscompute_stg_sample') }}) as sample

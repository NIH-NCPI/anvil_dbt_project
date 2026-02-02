{{ config(materialized='table', schema='alscompute_data') }}

select 
  {{ generate_global_id(prefix='sm',descriptor=['sample_id'], study_id='alscompute') }}::text as "sample_id",
  sample_id::text as "external_id"
from {{ ref('alscompute_stg_sample') }} as sample

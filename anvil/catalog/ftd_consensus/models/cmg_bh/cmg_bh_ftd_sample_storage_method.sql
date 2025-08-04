{{ config(materialized='table', schema='cmg_bh_data') }}

select 
  {{ generate_global_id(prefix='sm',descriptor=['sample_id'], study_id='cmg_bh') }}::text as "sample_id",
  NULL.storage_method::text as "storage_method"
from {{ ref('cmg_bh_stg_sample') }} as sample  
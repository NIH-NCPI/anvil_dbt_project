{{ config(materialized='table', schema='alscompute_data') }}

select 
  id::text as "sample_id",
  NULL::text as "storage_method"
from {{ ref('alscompute_ftd_sample') }} as sample
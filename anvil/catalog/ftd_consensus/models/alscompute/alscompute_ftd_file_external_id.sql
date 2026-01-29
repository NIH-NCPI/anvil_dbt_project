{{ config(materialized='table', schema='alscompute_data') }}

select 
  id::text as "file_id",
  filename::text as "external_id"
from (select distinct id, filename from {{ ref('alscompute_ftd_file') }}) as file
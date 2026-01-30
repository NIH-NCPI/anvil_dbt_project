{{ config(materialized='table', schema='alscompute_data') }}

with unioned_file_ids as (
        select
            distinct 
            name as "file_id",
        from {{ ref('alscompute_stg_file_inventory') }}
    
        union all
   
        select
            distinct 
            sample_id as "file_id",
        from {{ ref('alscompute_stg_sample') }}
)

select 
  {{ generate_global_id(prefix='fd',descriptor=['file_id'], study_id='alscompute') }}::text as "filemetadata_id",
  file_id::text as "external_id"
from unioned_file_ids
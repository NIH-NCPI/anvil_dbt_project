{{ config(materialized='table', schema='cmg_yale_data') }}

with 
get_sm_files as (
  select crai as file, sample_id 
  from {{ ref('cmg_yale_stg_sample') }}
    union all 
  select cram as file, sample_id 
  from {{ ref('cmg_yale_stg_sample') }}
)

select 
 distinct 
 {{ generate_global_id(prefix='sm',descriptor=['sample_id'], study_id='cmg_yale') }}::text as "sample_id",
 {{ generate_global_id(prefix='fl',descriptor=['name'], study_id='cmg_yale') }}::text as "file_id"
from 
  {{ ref('cmg_yale_stg_file_inventory') }}
  left join get_sm_files 
    on file = file_ref
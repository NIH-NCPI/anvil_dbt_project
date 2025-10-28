{{ config(materialized='table', schema='cmg_uwash_data') }}

select 
  {{ generate_global_id(prefix='st',descriptor=['dataset_id'], study_id='phs000693') }}::text as "study_id",
  NULL::text as "funding_source"
from {{ ref('cmg_uwash_stg_anvil_dataset') }} as anvil_dataset
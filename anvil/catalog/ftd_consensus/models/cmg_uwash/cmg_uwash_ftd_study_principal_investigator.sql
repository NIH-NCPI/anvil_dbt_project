{{ config(materialized='table', schema='cmg_uwash_data') }}

select 
  {{ generate_global_id(prefix='st',descriptor=['dataset_id'], study_id='phs000693') }}::text as "study_id",
  principal_investigator::text as "principal_investigator"
from {{ ref('cmg_uwash_stg_anvil_dataset') }} as anvil_dataset
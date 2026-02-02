{{ config(materialized='table', schema='alscompute_data') }}

select 
  NULL::text as "parent_study_id",
  title::text as "study_title",
    {{ generate_global_id(prefix='st',descriptor=['registered_identifier'], study_id='alscompute') }}::text as "id"
from {{ ref('alscompute_stg_anvil_dataset') }} as anvil_dataset

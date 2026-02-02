{{ config(materialized='table', schema='alscompute_data') }}

select distinct
  {{ generate_global_id(prefix='st',descriptor=['registered_identifier'], study_id='alscompute') }}::text as "study_id",
  principal_investigator::text as "principal_investigator"
from {{ ref('alscompute_stg_anvil_dataset') }} as anvil_dataset

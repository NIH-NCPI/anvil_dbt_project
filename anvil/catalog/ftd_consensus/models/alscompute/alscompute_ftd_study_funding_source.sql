{{ config(materialized='table', schema='alscompute_data') }}

select distinct
  {{ generate_global_id(prefix='st',descriptor=['registered_identifier'], study_id='alscompute') }}::text as "study_id",
  NULL::text as "funding_source"
from {{ ref('alscompute_stg_anvil_dataset') }} as anvil_dataset
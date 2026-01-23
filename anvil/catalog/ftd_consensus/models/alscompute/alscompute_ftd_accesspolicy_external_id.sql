{{ config(materialized='table', schema='alscompute_data') }}

select 
  {{ generate_global_id(prefix='ap',descriptor=['consent_id'], study_id='alscompute') }}::text as "accesspolicy_id",
  consent_id::text as "external_id"
from (select distinct consent_id from {{ ref('alscompute_stg_anvil_dataset') }}) as anvil_dataset
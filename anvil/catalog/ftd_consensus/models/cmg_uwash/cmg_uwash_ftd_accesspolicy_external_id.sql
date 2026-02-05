{{ config(materialized='table', schema='cmg_uwash_data') }}

select distinct
  {{ generate_global_id(prefix='ap',descriptor=['consent_group'], study_id='phs000693') }}::text as "accesspolicy_id",
  consent_group::text as "external_id"
from {{ ref('cmg_uwash_stg_anvil_dataset') }} as ad
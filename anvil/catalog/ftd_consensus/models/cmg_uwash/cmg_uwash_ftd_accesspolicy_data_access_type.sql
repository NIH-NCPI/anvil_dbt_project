{{ config(materialized='table', schema='cmg_uwash_data') }}

select distinct
  {{ generate_global_id(prefix='ap',descriptor=['ad.consent_group'], study_id='phs000693') }}::text as "accesspolicy_id",
  data_access_type::text as "data_access_type"
from {{ ref('cmg_uwash_stg_anvil_dataset') }} as ad
left join {{ ref('ap_access_policy') }} as aap 
 on ('-' || ad.consent_group || '-') ILIKE '%-' || aap.consent_code || '-%'

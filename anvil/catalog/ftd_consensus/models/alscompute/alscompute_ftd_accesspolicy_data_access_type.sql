{{ config(materialized='table', schema='alscompute_data') }}

select 
  {{ generate_global_id(prefix='ap',descriptor=['a.consent_id'], study_id='alscompute') }}::text as "accesspolicy_id",
  data_access_type::text as "data_access_type"
from (select distinct consent_id from {{ ref('alscompute_stg_anvil_dataset') }}) as a
left join {{ ref('ap_access_policy') }} as ap
on lower(a.consent_id) = lower(ap.consent_code)
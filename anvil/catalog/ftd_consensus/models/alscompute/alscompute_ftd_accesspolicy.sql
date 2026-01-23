{{ config(materialized='table', schema='alscompute_data') }}

select 
NULL::text as "disease_limitation",
ap.consent_description::text as "description",
NULL::text as "website",
    {{ generate_global_id(prefix='ap',descriptor=['a.consent_id'], study_id='alscompute') }}::text as "id"
from (select distinct consent_id from {{ ref('alscompute_stg_anvil_dataset') }}) as a
left join {{ ref('ap_access_policy') }} as ap
on lower(a.consent_id) = lower(ap.consent_code)
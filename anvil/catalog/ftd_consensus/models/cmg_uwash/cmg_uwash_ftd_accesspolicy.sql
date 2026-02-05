{{ config(materialized='table', schema='cmg_uwash_data') }}

select distinct
aap.disease_limitation::text as "disease_limitation",
  aap.consent_description::text as "description",
  NULL::text as "website",
    {{ generate_global_id(prefix='ap',descriptor=['ad.consent_group'], study_id='phs000693') }}::text as "id"
from  {{ ref('cmg_uwash_stg_anvil_dataset') }} as ad
left join {{ ref('ap_access_policy') }} as aap 
 on ('-' || ad.consent_group || '-') ILIKE '%-' || aap.consent_code || '-%'

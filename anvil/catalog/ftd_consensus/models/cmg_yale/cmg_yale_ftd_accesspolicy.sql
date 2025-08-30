{{ config(materialized='table', schema='cmg_yale_data') }}

select 
    distinct
    case
    when consent_group not ilike 'DS-%'
    then null
    when consent_group ilike 'DS-%'
    then REPLACE(REPLACE(consent_group,'DS-',''),'-IRB','')
    when consent_group is null
    then CONCAT('FTD_FLAG:unhandled description-null: ', consent_group)
    else CONCAT('FTD_FLAG:unhandled description: ',consent_group)
  end as "disease_limitation",
  consent_group as "ftd_consent_group",
  consent_description::text as "description",
  NULL::text as "website",
  {{ generate_global_id(prefix='ap',descriptor=['consent_group'], study_id='cmg_yale') }}::text as "id"
from (select distinct ad.consent_group, cc.consent_description
      from {{ ref('cmg_yale_stg_anvil_dataset') }} as ad
      left join {{ ref('ap_access_code') }} as cc
      on cc.consent_code = ad.consent_group
      ) as s
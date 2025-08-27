{{ config(materialized='table', schema='cmg_yale_data') }}

select 
    distinct
    case
    when consent_group not ilike 'DS-%'
    then null
    when consent_group ilike 'DS-%'
    then REPLACE(consent_group,'DS-','')
    else CONCAT('FTD_FLAG:unhandled description: ',consent_group)
  end as "disease_limitation",
  consent_group,
  consent_title::text as "description",
  NULL::text as "website",
  {{ generate_global_id(prefix='ap',descriptor=['consent_group'], study_id='cmg_yale') }}::text as "id"
from (select distinct ad.consent_group, cc.consent_title
      from {{ ref('cmg_yale_stg_anvil_dataset') }} as ad
      left join {{ ref('consent_codes') }} as cc
      on cc.consent_abbreviation = ad.consent_group
      ) as s
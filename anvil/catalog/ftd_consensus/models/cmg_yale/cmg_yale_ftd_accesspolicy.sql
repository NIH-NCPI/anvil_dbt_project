{{ config(materialized='table', schema='cmg_yale_data') }}

select 
  NULL::text as "disease_limitation",
    case
    when consent_id like '%ANVIL_CMG_Yale_GRU%'
    then 'General Research Use'
    when consent_id like '%AnVIL_CMG_Yale_DS-BPEAKD%'
    then 'Disease-Specific (Disease/Trait/Exposure)-BPEAKD'
    when consent_id like '%AnVIL_CMG_Yale_HMB-GSO%'
    then 'Health/Medical/Biomedical-GSO'
    when consent_id like '%AnVIL_CMG_Yale_HMB%'
    then 'Health/Medical/Biomedical'
    when consent_id like '%AnVIL_CMG_Yale_DS-THAL-IRB%'
    then 'Disease-Specific (Disease/Trait/Exposure)-THAL-IRB Approval Required'
    when consent_id like '%ANVIL_CMG_YALE_DS-RARED%'
    then 'Disease-Specific (Disease/Trait/Exposure)-RARED'
    when consent_id like '%AnVIL_CMG_Yale_DS-RD%'
    then 'Disease-Specific (Disease/Trait/Exposure)-RD'
    when consent_id like '%ANVIL_CMG_YALE_DS-MC%'
    then 'Disease-Specific (Disease/Trait/Exposure)-MC'
    when consent_id like '%AnVIL_CMG_Yale_HMB-IRB%'
    then 'Health/Medical/Biomedical-IRB Approval Required'
    else NULL
  end::text as "description", --TODO Get the rest of the descriptions
  NULL::text as "website",
  {{ generate_global_id(prefix='ap',descriptor=['consent_id'], study_id='cmg_yale') }}::text as "id"
from (select distinct consent_id from {{ ref('cmg_yale_stg_subject') }}) as s
{{ config(materialized='table', schema='cmg_uwash_data') }}

with unioned_codes as (
    select 
    subject_id,
    affected_status::text as "value_display",
    UNNEST(SPLIT(replace(disease_id, ';',''), '|')) as code,
  from {{ ref('cmg_uwash_stg_subject') }}
 
 union all
 
 select 
    subject_id,
    affected_status::text as "value_display",
    UNNEST(SPLIT(replace(hpo_present, ';',''), '|')) as code,
  from {{ ref('cmg_uwash_stg_subject') }}
     )
 
select 
  CASE 
    WHEN code ilike '%OMIM%' THEN 'disease'
    WHEN code ilike '%HP%' THEN 'phenotypic_feature'
    ELSE NULL
  END::text as "assertion_type",
  NULL as age_at_assertion,
  NULL as age_at_event,
  NULL as age_at_resolution,
  code,
  cua.display as display,
  CASE 
        WHEN LOWER(affected_status) = 'affected' THEN 'LA9633-4'
        WHEN LOWER(affected_status) = 'unaffected' THEN 'LA9634-2'
        WHEN LOWER(affected_status) = 'unknown' THEN 'LA4489-6'
        WHEN LOWER(affected_status) = 'possibly affected' THEN 'LA15097-1'
        WHEN affected_status is null THEN 'LA4489-6'
        ELSE CONCAT('FTD_FLAG: unhandled value_code: ', affected_status)
  END::text as "value_code",
  CASE 
        WHEN LOWER(affected_status) = 'affected' THEN 'Present'
        WHEN LOWER(affected_status) = 'unaffected' THEN 'Absent'
        WHEN LOWER(affected_status) = 'unknown' THEN 'Unknown'
        WHEN LOWER(affected_status) = 'possibly affected' THEN 'Possible'
        WHEN affected_status is null THEN 'Unknown'
        ELSE CONCAT('FTD_FLAG: unhandled value_code: ', affected_status)
  END::text as "value_display",
  NULL as value_number,
  NULL as value_units,
  NULL as value_units_display,
    {{ generate_global_id(prefix='ap',descriptor=['consent_id'], study_id='phs000693') }}::text as "has_access_policy",
    {{ generate_global_id(prefix='sa',descriptor=['subject_id', 'code'], study_id='phs000693') }}::text as "id",
    subject_id
--     { { generate_global_id(prefix='sb',descriptor=['subject_id', 'consent_id'], study_id='phs000693') }}::text as "subject_id"
from {{ ref('cmg_uwash_stg_subject') }} as s
left join unioned_codes as uc
using(subject_id)
left join {{ ref('cmg_uwash_annotations') }} as cua
on uc.code = cua.searched_code
{{ config(materialized='table', schema='cmg_uwash_data') }}

with unioned_codes as (
    select 
    subject_id,
    affected_status as "value_display",
    UNNEST(SPLIT(replace(disease_id, ';',''), '|')) as code,
    disease_description as "display",
    'disease' as assertion_type
  from {{ ref('cmg_uwash_stg_subject') }}
 
 union 
 
 select 
    subject_id,
    affected_status as "value_display",
    UNNEST(SPLIT(replace(hpo_present, ';',''), '|')) as code,
    phenotype_description as "display",
    'phenotypic_feature' as assertion_type
  from {{ ref('cmg_uwash_stg_subject') }}
     ),
     
all_codes as (select
    uc.subject_id as subject_id,
    uc.value_display as value_display,
    CASE 
        WHEN cua.response_code NOT LIKE '%:%' THEN concat('OMIM:', cua.response_code)
        ELSE cua.response_code
    END as code,
    cua.display as display,
    null as assertion_type,
    NULL as system
    from unioned_codes as uc
    join {{ ref('cmg_uwash_annotations') }} as cua
    on uc.code = cua.searched_code
         
         
    union
    
    select
    subject_id,
    uc.value_display::text as "value_display",
    cm.code as code,
    cm.display as display,
    CASE "local code system"
        WHEN 'CMG/phenotype_description' THEN 'phenotypic_feature'
        WHEN 'CMG/disease_description' THEN 'disease'
    END::text as assertion_type,
    cm."code system" as system
    from unioned_codes as uc
    left join {{ ref('cmg_uwash_condition_mappings') }} as cm
    on lower(uc.display) = lower(cm."local code")
)
  
 select 
  assertion_type,
  NULL as age_at_assertion,
  NULL as age_at_event,
  NULL as age_at_resolution,
  CASE
      WHEN code NOT LIKE '%:%' AND system ILIKE '%omim%' THEN concat('OMIM:', code)
      WHEN code NOT LIKE '%:%' AND system ILIKE '%snomed%' THEN concat('SNOMED:', code)
      ELSE code
  END as code,
  display,
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
left join all_codes as alc
using(subject_id)
where code is not null
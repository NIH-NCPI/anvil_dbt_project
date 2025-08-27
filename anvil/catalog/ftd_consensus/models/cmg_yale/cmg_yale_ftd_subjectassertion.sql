{{ config(materialized='table', schema='cmg_yale_data') }}

with
lookup as (
    select -- annotations from src
      "searched_code" as join_code,
      "searched_code" as code,
      "display" as display
    from {{ ref('conditions_annotations') }}
)

select 
  distinct
    case
    when c_or_d = 'disease'
      then 'disease'
    when c_or_d = 'condition'
      then 'phenotypic_feature'
    else null
  end::text as "assertion_type",
  NULL::text as "age_at_assertion",
  NULL::text as "age_at_event",
  NULL::text as "age_at_resolution",
  condition_or_disease_code as "code",
    case
    when c_or_d = 'disease'
      then disease_description
    when c_or_d = 'condition'
      then lookup.display
    when c_or_d is null
      then null
    else CONCAT('FTD_FLAG:unhandled display: ',condition_or_disease_code) 
  end::text as "display",
    case 
    when lower(presence) = 'affected'          then 'LA9633-4'
    when lower(presence) = 'unaffected'        then 'LA9634-2'
    when lower(presence) = 'unknown'           then 'LA4489-6'
    when lower(presence) = 'possibly affected' then 'LA15097-1'
    when presence is null                    then 'LA4489-6'
    else CONCAT('FTD_FLAG:unhandled value_code: ',presence)
  end::text as "value_code",
    case 
    when lower(presence) = 'affected'          then 'Affected'
    when lower(presence) = 'unaffected'        then 'Unaffected'
    when lower(presence) = 'unknown'           then 'Unknown'
    when lower(presence) = 'possibly affected' then 'Possible'
    when presence is null                    then 'Unknown'
    else CONCAT('FTD_FLAG:unhandled value_display: ',presence)
  end::text as "value_display",  
  NULL::text as "value_number",
  NULL::text as "value_units",
  NULL::text as "value_units_display",
  {{ generate_global_id(prefix='ap',descriptor=['consent_id'], study_id='cmg_yale') }}::text as "has_access_policy",
  {{ generate_global_id(prefix='sa',descriptor=['subject_id','condition_or_disease_code'], study_id='cmg_yale') }}::text as "id",
  {{ generate_global_id(prefix='sb',descriptor=['subject_id'], study_id='cmg_yale') }}::text as "subject_id"
from (select 
        distinct 
        consent_id,
        subject_id,
        c_or_d,
        condition_or_disease_code,
        disease_description,
        presence 
        from {{ ref('cmg_yale_stg_subject') }}
     ) as s
left join lookup
on s.condition_or_disease_code = lookup.join_code
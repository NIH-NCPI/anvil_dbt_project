

with
lookup as (
    select
      "searched_code" as join_code,
      "searched_code" as code,
      "display" as display
    from "dbt"."main"."cmg_yale_conditions_annotations"
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
  
  coalesce(code,'LA4489-6') AS "value_code",
  coalesce(presence, 'FTD_NULL') AS "ftd_value_code", -- flag nulls for analysis
  coalesce(code, 'Needs Handling') AS "ftd_value_code", -- flag unhandled strings
  
  coalesce(display,'Unknown') AS "value_display",
  NULL::text as "value_number",
  NULL::text as "value_units",
  NULL::text as "value_units_display",
  'ap' || '_' || md5('cmg_yale' || '|' || cast(coalesce(consent_id, '') as text))::text as "has_access_policy",
  'sa' || '_' || md5('cmg_yale' || '|' || cast(coalesce(subject_id, '') as text) || '|' || 'cmg_yale' || '|' || cast(coalesce(condition_or_disease_code, '') as text))::text as "id",
  'sb' || '_' || md5('cmg_yale' || '|' || cast(coalesce(subject_id, '') as text))::text as "subject_id"
from (select 
        distinct 
        consent_id,
        subject_id,
        c_or_d,
        condition_or_disease_code,
        disease_description,
        presence 
        from "dbt"."main_main"."cmg_yale_stg_subject"
     ) as s
left join lookup
on s.condition_or_disease_code = lookup.join_code
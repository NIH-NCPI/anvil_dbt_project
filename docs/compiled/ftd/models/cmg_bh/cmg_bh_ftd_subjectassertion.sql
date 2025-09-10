

with
lookup as (
    select -- annotations from src
      "searched_code" as join_code,
      "searched_code" as code,
      "display" as display
    from "dbt"."main"."cmg_bh_annotations_code"
    
    union all
    
    select -- annotations from MD
      "local code" as join_code,
      "code" as code,
      "display" as display
    from "dbt"."main"."subject_mappings"
)

select 
  distinct
  'phenotypic_feature'::text as "assertion_type",
  NULL::text as "age_at_assertion",
  NULL::text as "age_at_event",
  NULL::text as "age_at_resolution",
  lookup.code as "code",
  lookup.display as "display",
    case lower(presence)
    when 'affected' then 'LA9633-4'
    when 'unaffected' then 'LA9634-2'
    when 'unknown' then 'LA4489-6'
    when presence is null then 'LA4489-6'
    else CONCAT('FTD_FLAG:unhandled value_code: ',presence)
  end::text as "value_code",
    case lower(presence)
    when 'affected' then 'Affected'
    when 'unaffected' then 'Unaffected'
    when 'unknown' then 'Unknown'
    when presence is null then 'Unknown'
    else CONCAT('FTD_FLAG:unhandled value_display: ',presence)
  end::text as "value_display",  
  NULL::text as "value_number",
  NULL::text as "value_units",
  NULL::text as "value_units_display",
  'ap' || '_' || md5('cmg_bh' || '|' || cast(coalesce(ingest_provenance, '') as text))::text as "has_access_policy",
  'sa' || '_' || md5('cmg_bh' || '|' || cast(coalesce(subject_id, '') as text) || '|' || 'cmg_bh' || '|' || cast(coalesce(condition_or_disease_code, '') as text))::text as "id",
  'sb' || '_' || md5('cmg_bh' || '|' || cast(coalesce(subject_id, '') as text))::text as "subject_id"
from (select distinct ingest_provenance, subject_id, condition_or_disease_code, presence from "dbt"."main_main"."cmg_bh_stg_subject") as s
join lookup
on s.condition_or_disease_code = lookup.join_code
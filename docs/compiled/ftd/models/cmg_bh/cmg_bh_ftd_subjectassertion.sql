

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
    case
    when presence = 'Affected' then 'affected'
    when presence = 'Unaffected' then 'unaffected'
    when presence = 'Unknown' then 'unknown'
    else CONCAT('FTD_FLAG:unhandled value_code: ',presence)
  end::text as "value_code",
    case
    when presence = 'Affected' then 'Affected'
    when presence = 'Unaffected' then 'Unaffected'
    when presence = 'Unknown' then 'Unknown'
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
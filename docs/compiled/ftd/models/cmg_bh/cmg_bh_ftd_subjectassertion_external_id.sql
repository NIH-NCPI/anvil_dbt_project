

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
   'sa' || '_' || md5('cmg_bh' || '|' || cast(coalesce(subject_id, '') as text) || '|' || 'cmg_bh' || '|' || cast(coalesce(code, '') as text))::text as "subjectassertion_id",
   NULL::text as "external_id" -- Always null
from (select distinct subject_id, dbgap_study_id from "dbt"."main_main"."cmg_bh_stg_subject" ) as s
left join lookup
on code = lookup.join_code
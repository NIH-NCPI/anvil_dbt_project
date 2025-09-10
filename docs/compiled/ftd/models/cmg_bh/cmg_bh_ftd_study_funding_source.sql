

select 
  'st' || '_' || md5('cmg_bh' || '|' || cast(coalesce(dbgap_study_id, '') as text))::text as "study_id",
  NULL::text as "funding_source"
from (select distinct dbgap_study_id from "dbt"."main_main"."cmg_bh_stg_subject") as s
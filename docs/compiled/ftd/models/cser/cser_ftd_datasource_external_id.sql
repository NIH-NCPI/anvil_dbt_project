

select distinct
  'ds' || '_' || md5('cser' || '|' || cast(coalesce(dbgap_study_id, '') as text))::text as "datasource_id",
  dbgap_study_id::text as "external_id"
from "dbt"."main_main"."cser_stg_subject" as subject
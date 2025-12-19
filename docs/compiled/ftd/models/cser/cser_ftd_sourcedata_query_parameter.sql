

select 
  'sd' || '_' || md5('cser' || '|' || cast(coalesce(dbgap_study_id, '') as text))::text as "sourcedata_id",
  NULL::text as "query_parameter"
from (select distinct dbgap_study_id 
      from "dbt"."main_main"."cser_stg_subject"
      ) as s
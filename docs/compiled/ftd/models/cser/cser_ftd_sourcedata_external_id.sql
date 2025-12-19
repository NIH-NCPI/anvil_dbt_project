

select 
  'sd' || '_' || md5('cser' || '|' || cast(coalesce(dbgap_study_id, '') as text))::text as "sourcedata_id",
  dbgap_study_id::text as "external_id"
from (select distinct dbgap_study_id, consent_id 
      from "dbt"."main_main"."cser_stg_subject"
      ) as s
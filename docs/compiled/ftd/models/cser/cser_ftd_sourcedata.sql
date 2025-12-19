

select 
  'ds' || '_' || md5('cser' || '|' || cast(coalesce(dbgap_study_id, '') as text))::text as "data_source",
    'ap' || '_' || md5('cser' || '|' || cast(coalesce(consent_id, '') as text))::text as "has_access_policy",
    'sd' || '_' || md5('cser' || '|' || cast(coalesce(dbgap_study_id, '') as text))::text as "id"
from  (select distinct dbgap_study_id, consent_id 
      from "dbt"."main_main"."cser_stg_subject"
      ) as s
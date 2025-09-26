
  
select 
  'st' || '_' || md5('cmg_yale' || '|' || cast(coalesce(dbgap_study_id, '') as text))::text as "study_id",
  'dm' || '_' || md5('cmg_yale' || '|' || cast(coalesce(subject_id, '') as text))::text as "demographics_id",
  'sd' || '_' || md5('cmg_yale' || '|' || cast(coalesce(project_id, '') as text))::text as "source_data_id"
  
from (select distinct subject_id, project_id, dbgap_study_id 
      from "dbt"."main_main"."cmg_yale_stg_subject" 
      where dbgap_study_id is not null) as s
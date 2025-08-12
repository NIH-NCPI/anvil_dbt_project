
  
    
    

    create  table
      "dbt"."main_cmg_bh_data"."cmg_bh_ftd_study_principal_investigator__dbt_tmp"
  
    as (
      

select 
  'st' || '_' || md5('cmg_bh' || '|' || cast(coalesce(dbgap_study_id, '') as text))::text as "study_id",
  project_investigator::text as "principal_investigator"
from  (select distinct dbgap_study_id, project_investigator from "dbt"."main_main"."cmg_bh_stg_subject") as s
    );
  
  
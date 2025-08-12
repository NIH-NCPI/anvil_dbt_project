
  
    
    

    create  table
      "dbt"."main_cmg_bh_data"."cmg_bh_ftd_datasource_external_id__dbt_tmp"
  
    as (
      

select 
  'ds' || '_' || md5('cmg_bh' || '|' || cast(coalesce(dbgap_study_id, '') as text))::text as "datasource_id",
  dbgap_study_id::text as "external_id"
from (select distinct dbgap_study_id from "dbt"."main_main"."cmg_bh_stg_subject" ) as s
    );
  
  
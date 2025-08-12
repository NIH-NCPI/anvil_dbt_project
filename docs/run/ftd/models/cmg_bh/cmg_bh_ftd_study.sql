
  
    
    

    create  table
      "dbt"."main_cmg_bh_data"."cmg_bh_ftd_study__dbt_tmp"
  
    as (
      

select 
  NULL::text as "parent_study_id",
  'Baylor Hopkins Center for Mendelian Genomics (BH CMG)' as "study_title", 
  'st' || '_' || md5('cmg_bh' || '|' || cast(coalesce(dbgap_study_id, '') as text))::text as "id"
from (select distinct dbgap_study_id from "dbt"."main_main"."cmg_bh_stg_subject") s
    );
  
  

  
    
    

    create  table
      "dbt"."main_cmg_bh_data"."cmg_bh_ftd_subject_external_id__dbt_tmp"
  
    as (
      

select 
  'sb' || '_' || md5('cmg_bh' || '|' || cast(coalesce(subject_id, '') as text))::text as "subject_id",
  subject_id::text as "external_id"
from (select distinct subject_id from "dbt"."main_main"."cmg_bh_stg_subject" ) as s
    );
  
  

  
    
    

    create  table
      "dbt"."main_cmg_bh_data"."cmg_bh_ftd_subjectassertion_source_data__dbt_tmp"
  
    as (
      

select
  distinct
  'sa' || '_' || md5('cmg_bh' || '|' || cast(coalesce(subject_id, '') as text) || '|' || 'cmg_bh' || '|' || cast(coalesce(condition_or_disease_code, '') as text))::text as "subjectassertion_id",
  'sd' || '_' || md5('cmg_bh' || '|' || cast(coalesce(dbgap_study_id, '') as text))::text as "source_data_id"
from (select distinct subject_id, condition_or_disease_code, dbgap_study_id from "dbt"."main_main"."cmg_bh_stg_subject" ) as s
    );
  
  
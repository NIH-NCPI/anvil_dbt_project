
  
    
    

    create  table
      "dbt"."main_cmg_bh_data"."cmg_bh_ftd_sourcedata__dbt_tmp"
  
    as (
      

select 
  'ds' || '_' || md5('cmg_bh' || '|' || cast(coalesce(dbgap_study_id, '') as text))::text as "data_source",
  'ap' || '_' || md5('cmg_bh' || '|' || cast(coalesce(ingest_provenance, '') as text))::text as "has_access_policy",
  'sd' || '_' || md5('cmg_bh' || '|' || cast(coalesce(dbgap_study_id, '') as text))::text as "id"
from (select distinct dbgap_study_id, ingest_provenance from "dbt"."main_main"."cmg_bh_stg_subject" ) as s
    );
  
  
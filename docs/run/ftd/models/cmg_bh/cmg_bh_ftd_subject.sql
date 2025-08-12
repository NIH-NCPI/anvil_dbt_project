
  
    
    

    create  table
      "dbt"."main_cmg_bh_data"."cmg_bh_ftd_subject__dbt_tmp"
  
    as (
      

select 
  distinct
  'participant'::text as "subject_type",
  'human'::text as "organism_type",
  'ap' || '_' || md5('cmg_bh' || '|' || cast(coalesce(ingest_provenance, '') as text))::text as "has_access_policy",
  'sb' || '_' || md5('cmg_bh' || '|' || cast(coalesce(subject_id, '') as text))::text as "id",
  'dm' || '_' || md5('cmg_bh' || '|' || cast(coalesce(subject_id, '') as text))::text as "has_demographics_id"
from (select distinct subject_id, ingest_provenance from "dbt"."main_main"."cmg_bh_stg_subject" ) AS s
    );
  
  
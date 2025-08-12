
  
    
    

    create  table
      "dbt"."main_cmg_bh_data"."cmg_bh_ftd_family__dbt_tmp"
  
    as (
      

select 
  NULL::text as "family_type",
  NULL::text as "family_description",
  NULL::text as "consanguinity",
  NULL::text as "family_study_focus",
  'ap' || '_' || md5('cmg_bh' || '|' || cast(coalesce(ingest_provenance, '') as text))::text as "has_access_policy",
  'fy' || '_' || md5('cmg_bh' || '|' || cast(coalesce(family_id, '') as text))::text as "id"
from (select distinct ingest_provenance, family_id from "dbt"."main_main"."cmg_bh_stg_subject")
    );
  
  
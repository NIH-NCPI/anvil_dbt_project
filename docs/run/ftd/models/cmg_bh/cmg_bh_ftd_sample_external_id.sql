
  
    
    

    create  table
      "dbt"."main_cmg_bh_data"."cmg_bh_ftd_sample_external_id__dbt_tmp"
  
    as (
      

select 
  distinct
  'sm' || '_' || md5('cmg_bh' || '|' || cast(coalesce(sample_id, '') as text) || '|' || 'cmg_bh' || '|' || cast(coalesce(subject_id, '') as text))::text as "sample_id",
  sample_id::text as "external_id"
from (select distinct sample_id, subject_id, ingest_provenance
    from (select distinct subject_id, ingest_provenance, sample_id from "dbt"."main_main"."cmg_bh_stg_sample") as sam
    join (select distinct subject_id, ingest_provenance from "dbt"."main_main"."cmg_bh_stg_subject") as sub
    using (subject_id, ingest_provenance)
      )
    );
  
  
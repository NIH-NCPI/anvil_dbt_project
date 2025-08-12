
  
    
    

    create  table
      "dbt"."main_main"."cmg_bh_stg_sample__dbt_tmp"
  
    as (
      

    with source as (
        select 
        "sample_id"::text as "sample_id",
       "bai"::text as "bai",
       "bai_md5"::text as "bai_md5",
       "bam"::text as "bam",
       "bam_md5"::text as "bam_md5",
       "subject_id"::text as "subject_id",
       "crai"::text as "crai",
       "cram_md5"::text as "cram_md5",
       "ingest_provenance"::text as "ingest_provenance",
       "Submission_Batch"::text as "submission_batch",
       "dbgap_sample_id"::text as "dbgap_sample_id",
       "sample_provider"::text as "sample_provider",
       "sample_source"::text as "sample_source",
       "tissue_affected_status"::text as "tissue_affected_status"
        from "dbt"."main"."sample"
    )

    select 
        ROW_NUMBER() OVER () AS ftd_index
        ,source.*
        from source
    );
  
  
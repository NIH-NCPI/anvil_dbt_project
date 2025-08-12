
  
    
    

    create  table
      "dbt"."main_cmg_bh_data"."cmg_bh_ftd_file_external_id__dbt_tmp"
  
    as (
      with 
unpivot_df as (select
            distinct
            ingest_provenance,
            'bai' as "file_type",
            cast(bai as varchar) as "drs_uri"
        from "dbt"."main_main"."cmg_bh_stg_sample"
        where bai IS NOT NULL
        union all
    select
            distinct
            ingest_provenance,
            'bai_md5' as "file_type",
            cast(bai_md5 as varchar) as "drs_uri"
        from "dbt"."main_main"."cmg_bh_stg_sample"
        where bai_md5 IS NOT NULL
        union all
    select
            distinct
            ingest_provenance,
            'bam' as "file_type",
            cast(bam as varchar) as "drs_uri"
        from "dbt"."main_main"."cmg_bh_stg_sample"
        where bam IS NOT NULL
        union all
    select
            distinct
            ingest_provenance,
            'bam_md5' as "file_type",
            cast(bam_md5 as varchar) as "drs_uri"
        from "dbt"."main_main"."cmg_bh_stg_sample"
        where bam_md5 IS NOT NULL
        union all
    select
            distinct
            ingest_provenance,
            'crai' as "file_type",
            cast(crai as varchar) as "drs_uri"
        from "dbt"."main_main"."cmg_bh_stg_sample"
        where crai IS NOT NULL
        union all
    select
            distinct
            ingest_provenance,
            'cram_md5' as "file_type",
            cast(cram_md5 as varchar) as "drs_uri"
        from "dbt"."main_main"."cmg_bh_stg_sample"
        where cram_md5 IS NOT NULL
        
    
)
select 
  'fl' || '_' || md5('cmg_bh' || '|' || cast(coalesce(drs_uri, '') as text))::text as "file_id",
  drs_uri::text as "external_id"
from unpivot_df
    );
  
  
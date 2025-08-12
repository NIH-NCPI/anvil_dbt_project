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
  drs_uri::text as "filename",
  NULL::text as "format",
  file_type::text as "data_type",
  NULL::integer as "size",
  drs_uri::text as "drs_uri",
  'fm' || '_' || md5('cmg_bh' || '|' || cast(coalesce(drs_uri, '') as text))::text as "file_metadata",
  'ap' || '_' || md5('cmg_bh' || '|' || cast(coalesce(ingest_provenance, '') as text))::text as "has_access_policy",
  'fl' || '_' || md5('cmg_bh' || '|' || cast(coalesce(drs_uri, '') as text))::text as "id"
from unpivot_df


select 
  distinct
  'sm' || '_' || md5('cmg_bh' || '|' || cast(coalesce(sample_id, '') as text) || '|' || 'cmg_bh' || '|' || cast(coalesce(subject_id, '') as text))::text as "sample_id",
  sample_id::text as "external_id"
from (select distinct sample_id, subject_id, sample_source, sam.ingest_provenance as ingest_provenance
    from (
        select distinct subject_id, replace(ingest_provenance,'sample_','') as access, ingest_provenance, sample_source, sample_id 
        from "dbt"."main_main"."cmg_bh_stg_sample"
        ) as sam
    left join (
        select distinct subject_id, replace(ingest_provenance,'subject_','') as access, ingest_provenance 
        from "dbt"."main_main"."cmg_bh_stg_subject"
        ) as sub
    using (subject_id, access)
    ) as f
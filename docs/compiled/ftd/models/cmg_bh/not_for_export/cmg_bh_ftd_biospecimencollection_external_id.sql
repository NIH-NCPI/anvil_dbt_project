

select
  sample_id::text as "external_id",
  'bc' || '_' || md5('cmg_bh' || '|' || cast(coalesce(sample_id, '') as text) || '|' || 'cmg_bh' || '|' || cast(coalesce(sample_source, '') as text))::text as "biospecimencollection_id"
from (select distinct sample_id, sample_source, ingest_provenance
    from (select distinct subject_id, sample_source, sample_id from "dbt"."main_main"."cmg_bh_stg_sample") as sam
    join (select distinct subject_id, ingest_provenance from "dbt"."main_main"."cmg_bh_stg_subject") as sub
    using (subject_id)
      )
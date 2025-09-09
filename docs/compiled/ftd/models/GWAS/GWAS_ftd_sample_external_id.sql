

select DISTINCT
  'sm' || '_' || md5('phs001584' || '|' || cast(coalesce(samplesubjectmapping.subject_id, '') as text) || '|' || 'phs001584' || '|' || cast(coalesce(samplesubjectmapping.source_sampid, '') as text))::text as  "sample_id",
  samplesubjectmapping.source_sampid::text as "external_id"
from "dbt"."main_main"."GWAS_stg_samplesubjectmapping" as samplesubjectmapping
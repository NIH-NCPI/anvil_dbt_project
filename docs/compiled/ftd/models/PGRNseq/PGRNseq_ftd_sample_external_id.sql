

select distinct
  'sm' || '_' || md5('phs000906' || '|' || cast(coalesce(samplesubjectmapping.subject_id, '') as text) || '|' || 'phs000906' || '|' || cast(coalesce(samplesubjectmapping.sample_id, '') as text) || '|' || 'phs000906' || '|' || cast(coalesce(sc.consent, '') as text))::text as "sample_id",
  samplesubjectmapping.sample_id::text as "external_id"
from "dbt"."main_main"."PGRNseq_stg_samplesubjectmapping" as samplesubjectmapping
left join "dbt"."main_main"."PGRNseq_stg_subjectconsent" as sc
using(subject_id)
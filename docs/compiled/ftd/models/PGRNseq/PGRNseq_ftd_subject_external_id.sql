

select distinct
  'sb' || '_' || md5('phs000906' || '|' || cast(coalesce(subjectconsent.subject_id, '') as text) || '|' || 'phs000906' || '|' || cast(coalesce(subjectconsent.consent, '') as text))::text as "subject_id",
  subjectconsent.subject_id::text as "external_id"
from "dbt"."main_main"."PGRNseq_stg_subjectconsent" as subjectconsent
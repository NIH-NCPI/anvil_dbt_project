

select 
  'dm' || '_' || md5('phs000906' || '|' || cast(coalesce(demographics.subject_id, '') as text) || '|' || 'phs000906' || '|' || cast(coalesce(sc.consent, '') as text))::text as "demographics_id",
  demographics.subject_id::text as "external_id"
from "dbt"."main_main"."PGRNseq_stg_demographics" as demographics
left join "dbt"."main_main"."PGRNseq_stg_subjectconsent" as sc
using(subject_id)
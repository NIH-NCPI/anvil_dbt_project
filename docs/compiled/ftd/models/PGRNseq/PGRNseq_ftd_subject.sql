

select distinct
'participant'::text as "subject_type",
NULL::text as "organism_type",
    'ap' || '_' || md5('phs000906' || '|' || cast(coalesce(subjectconsent.consent, '') as text))::text as "has_access_policy",
    'sb' || '_' || md5('phs000906' || '|' || cast(coalesce(subjectconsent.subject_id, '') as text) || '|' || 'phs000906' || '|' || cast(coalesce(subjectconsent.consent, '') as text))::text as "id",
    'dm' || '_' || md5('phs000906' || '|' || cast(coalesce(demographics.subject_id, '') as text) || '|' || 'phs000906' || '|' || cast(coalesce(subjectconsent.consent, '') as text))::text as "has_demographics_id"
from "dbt"."main_main"."PGRNseq_stg_subjectconsent" as subjectconsent
left join "dbt"."main_main"."PGRNseq_stg_demographics" as demographics
using (subject_id)
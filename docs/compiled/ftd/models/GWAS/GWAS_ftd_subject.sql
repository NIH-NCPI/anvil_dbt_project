

select 
  CASE 
      WHEN subjectconsent.consent != 0 THEN 'participant'
      ELSE 'non_participant'
  END::text as subject_type,  
  NULL as "organism_type",
    'ap' || '_' || md5('phs001584' || '|' || cast(coalesce(subjectconsent.consent, '') as text))::text as "has_access_policy",
    'sb' || '_' || md5('phs001584' || '|' || cast(coalesce(subjectconsent.subject_id, '') as text))::text as "id",
    'dm' || '_' || md5('phs001584' || '|' || cast(coalesce(demographics.subject_id, '') as text))::text as "has_demographics_id"
from "dbt"."main_main"."GWAS_stg_subjectconsent" as subjectconsent
join "dbt"."main_main"."GWAS_stg_demographics" as demographics
using (subject_id)
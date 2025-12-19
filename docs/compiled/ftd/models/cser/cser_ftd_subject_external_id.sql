

select 
  'sb' || '_' || md5('cser' || '|' || cast(coalesce(subject_id, '') as text) || '|' || 'cser' || '|' || cast(coalesce(consent_id, '') as text))::text as "subject_id",
  subject_id::text as "external_id"
from (select distinct subject_id, consent_id from "dbt"."main_main"."cser_stg_subject") as subject
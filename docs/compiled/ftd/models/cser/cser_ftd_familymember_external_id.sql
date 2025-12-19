

select distinct
  'fm' || '_' || md5('cser' || '|' || cast(coalesce(family_id, '') as text) || '|' || 'cser' || '|' || cast(coalesce(subject_id, '') as text) || '|' || 'cser' || '|' || cast(coalesce(consent_id, '') as text))::text as "familymember_id",
  subject_id::text as "external_id"
from "dbt"."main_main"."cser_stg_subject" as subject
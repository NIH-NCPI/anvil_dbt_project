

select distinct
  'sb' || '_' || md5('cser' || '|' || cast(coalesce(subject_id, '') as text) || '|' || 'cser' || '|' || cast(coalesce(consent_id, '') as text))::text as "family_member",
  NULL::text as "family_role",
  'ap' || '_' || md5('cser' || '|' || cast(coalesce(consent_id, '') as text))::text as "has_access_policy",
  'fm' || '_' || md5('cser' || '|' || cast(coalesce(family_id, '') as text) || '|' || 'cser' || '|' || cast(coalesce(subject_id, '') as text) || '|' || 'cser' || '|' || cast(coalesce(consent_id, '') as text))::text as "id",
  'fy' || '_' || md5('cser' || '|' || cast(coalesce(family_id, '') as text))::text as "family_id"
from "dbt"."main_main"."cser_stg_subject" as subject


select 
'participant'::text as "subject_type",
'human'::text as "organism_type",
    'ap' || '_' || md5('cser' || '|' || cast(coalesce(consent_id, '') as text))::text as "has_access_policy",
    'sb' || '_' || md5('cser' || '|' || cast(coalesce(subject_id, '') as text) || '|' || 'cser' || '|' || cast(coalesce(consent_id, '') as text))::text as "id",
    'dm' || '_' || md5('cser' || '|' || cast(coalesce(subject_id, '') as text) || '|' || 'cser' || '|' || cast(coalesce(consent_id, '') as text))::text as "has_demographics_id"
from (select distinct subject_id, consent_id from "dbt"."main_main"."cser_stg_subject") as subject
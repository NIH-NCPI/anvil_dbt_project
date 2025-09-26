

select 
  'participant'::text as "subject_type",
  'human'::text as "organism_type",
    'ap' || '_' || md5('cmg_yale' || '|' || cast(coalesce(consent_id, '') as text))::text as "has_access_policy",
    'sb' || '_' || md5('cmg_yale' || '|' || cast(coalesce(subject_id, '') as text))::text as "id",
    'dm' || '_' || md5('cmg_yale' || '|' || cast(coalesce(subject_id, '') as text))::text as "has_demographics_id"
from (select distinct subject_id, consent_id from "dbt"."main_main"."cmg_yale_stg_subject" ) AS s
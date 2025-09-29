

select 
  distinct
  'fm' || '_' || md5('cmg_yale' || '|' || cast(coalesce(family_id, '') as text) || '|' || 'cmg_yale' || '|' || cast(coalesce(subject_id, '') as text))::text as "familymember_id",
  subject_id::text as "external_id"
from (select distinct subject_id,family_id from "dbt"."main_main"."cmg_yale_stg_subject") s
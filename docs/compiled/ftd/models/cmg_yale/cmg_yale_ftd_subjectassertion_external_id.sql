

select 
  distinct
  'sa' || '_' || md5('cmg_yale' || '|' || cast(coalesce(subject_id, '') as text) || '|' || 'cmg_yale' || '|' || cast(coalesce(condition_or_disease_code, '') as text))::text as "subjectassertion_id",
  subject_id::text as "external_id"
from (select 
        distinct subject_id,condition_or_disease_code,
      from "dbt"."main_main"."cmg_yale_stg_subject"
      ) as s
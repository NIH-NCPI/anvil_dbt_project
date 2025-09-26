

select
  distinct
  'sa' || '_' || md5('cmg_yale' || '|' || cast(coalesce(subject_id, '') as text) || '|' || 'cmg_yale' || '|' || cast(coalesce(condition_or_disease_code, '') as text))::text as "subjectassertion_id",
  'sd' || '_' || md5('cmg_yale' || '|' || cast(coalesce(project_id, '') as text))::text as "source_data_id"
from (select distinct subject_id, condition_or_disease_code, project_id from "dbt"."main_main"."cmg_yale_stg_subject" ) as s
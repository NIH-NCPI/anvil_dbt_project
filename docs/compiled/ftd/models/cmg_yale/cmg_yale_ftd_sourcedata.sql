

select 
  distinct
--   { { generate_global_id(prefix='',descriptor=[''], study_id='cmg_yale') }}::text as "data_source",
  'ap' || '_' || md5('cmg_yale' || '|' || cast(coalesce(consent_id, '') as text))::text as "has_access_policy",
  'sd' || '_' || md5('cmg_yale' || '|' || cast(coalesce(project_id, '') as text))::text as "id",
  project_id as "ftd_project_id",
  NULL::text as "data_source"
  from (select distinct project_id, consent_id from "dbt"."main_main"."cmg_yale_stg_subject" ) AS s
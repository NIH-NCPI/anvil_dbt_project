

select 
  'sd' || '_' || md5('cmg_yale' || '|' || cast(coalesce(project_id, '') as text))::text as "sourcedata_id",
  project_id::text as "external_id"
from (select distinct project_id, consent_id from "dbt"."main_main"."cmg_yale_stg_subject" ) AS s
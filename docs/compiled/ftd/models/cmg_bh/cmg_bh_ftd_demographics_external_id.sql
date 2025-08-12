

select 
  'dm' || '_' || md5('cmg_bh' || '|' || cast(coalesce(subject_id, '') as text))::text as "demographics_id",
  subject_id::text as "external_id"
from (select distinct subject_id from "dbt"."main_main"."cmg_bh_stg_subject") as s
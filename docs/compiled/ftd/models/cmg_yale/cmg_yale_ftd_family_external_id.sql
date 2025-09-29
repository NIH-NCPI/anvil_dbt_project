

select 
  'fy' || '_' || md5('cmg_yale' || '|' || cast(coalesce(family_id, '') as text))::text as "family_id",
   family_id::text as "external_id"
from (select distinct family_id from "dbt"."main_main"."cmg_yale_stg_subject") as s


select distinct
  'fy' || '_' || md5('cser' || '|' || cast(coalesce(family_id, '') as text))::text as "family_id",
  family_id::text as "external_id"
from "dbt"."main_main"."cser_stg_subject" as subject
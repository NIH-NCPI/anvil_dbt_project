

select distinct
  'ap' || '_' || md5('cser' || '|' || cast(coalesce(consent_id, '') as text))::text as "accesspolicy_id",
  consent_id::text as "external_id"
from "dbt"."main_main"."cser_stg_subject" as subject
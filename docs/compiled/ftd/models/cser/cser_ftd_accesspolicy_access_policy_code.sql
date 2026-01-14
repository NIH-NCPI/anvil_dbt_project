

select distinct
  'ap' || '_' || md5('cser' || '|' || cast(coalesce(ingest_provenance, '') as text))::text as "accesspolicy_id",
  lower(consent_id)::text as "access_policy_code"
from "dbt"."main_main"."cser_stg_subject" as s
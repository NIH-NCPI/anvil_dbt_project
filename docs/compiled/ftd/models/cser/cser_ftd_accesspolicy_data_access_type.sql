

select distinct
  'ap' || '_' || md5('cser' || '|' || cast(coalesce(s.consent_id, '') as text))::text as "accesspolicy_id",
  data_access_type::text as "data_access_type"
from "dbt"."main_main"."cser_stg_subject" as s
left join "dbt"."main"."ap_access_policy" as ap
on lower(s.consent_id) = lower(ap.consent_code)
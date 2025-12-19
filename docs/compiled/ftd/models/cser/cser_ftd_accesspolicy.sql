

select distinct
NULL::text as "disease_limitation",
ap.consent_description::text as "description",
NULL::text as "website",
'ap' || '_' || md5('cser' || '|' || cast(coalesce(s.ingest_provenance, '') as text))::text as "id"
from "dbt"."main_main"."cser_stg_subject" as s
left join "dbt"."main"."ap_access_policy" as ap
on lower(s.consent_id) = lower(ap.consent_code)
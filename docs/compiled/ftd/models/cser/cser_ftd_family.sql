

select distinct
NULL::text as "family_type",
NULL::text as "family_description",
NULL::text as "consanguinity",
NULL::text as "family_study_focus",
    'ap' || '_' || md5('cser' || '|' || cast(coalesce(consent_id, '') as text))::text as "has_access_policy",
    'fy' || '_' || md5('cser' || '|' || cast(coalesce(family_id, '') as text))::text as "id"
from "dbt"."main_main"."cser_stg_subject" as subject
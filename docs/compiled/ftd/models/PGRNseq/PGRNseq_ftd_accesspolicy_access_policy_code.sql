(
    select DISTINCT 
    'ap' || '_' || md5('phs000906' || '|' || cast(coalesce(subjectconsent.consent, '') as text))::text as "accesspolicy_id",
    'hmb' as "access_policy_code"
from "dbt"."main_main"."PGRNseq_stg_subjectconsent" as subjectconsent
    where subjectconsent.consent in ('1')
        )
    union all(
    select DISTINCT 
    'ap' || '_' || md5('phs000906' || '|' || cast(coalesce(subjectconsent.consent, '') as text))::text as "accesspolicy_id",
    'ds' as "access_policy_code"
from "dbt"."main_main"."PGRNseq_stg_subjectconsent" as subjectconsent
    where subjectconsent.consent in ('2')
        )
    union all(
    select DISTINCT 
    'ap' || '_' || md5('phs000906' || '|' || cast(coalesce(subjectconsent.consent, '') as text))::text as "accesspolicy_id",
    'gru' as "access_policy_code"
from "dbt"."main_main"."PGRNseq_stg_subjectconsent" as subjectconsent
    where subjectconsent.consent in ('3')
        )
    
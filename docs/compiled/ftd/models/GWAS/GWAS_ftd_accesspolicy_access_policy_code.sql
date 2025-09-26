(
    select DISTINCT 
    'ap' || '_' || md5('phs001584' || '|' || cast(coalesce(subjectconsent.consent, '') as text))::text as "accesspolicy_id",
    'gru' as "access_policy_code"
from "dbt"."main_main"."GWAS_stg_subjectconsent" as subjectconsent
    where subjectconsent.consent in ('1', 
          '2', 
          '3', 
          '9')
        )
    union all(
    select DISTINCT 
    'ap' || '_' || md5('phs001584' || '|' || cast(coalesce(subjectconsent.consent, '') as text))::text as "accesspolicy_id",
    'irb' as "access_policy_code"
from "dbt"."main_main"."GWAS_stg_subjectconsent" as subjectconsent
    where subjectconsent.consent in ('2', 
          '3', 
          '9')
        )
    union all(
    select DISTINCT 
    'ap' || '_' || md5('phs001584' || '|' || cast(coalesce(subjectconsent.consent, '') as text))::text as "accesspolicy_id",
    'col' as "access_policy_code"
from "dbt"."main_main"."GWAS_stg_subjectconsent" as subjectconsent
    where subjectconsent.consent in ('3')
        )
    union all(
    select DISTINCT 
    'ap' || '_' || md5('phs001584' || '|' || cast(coalesce(subjectconsent.consent, '') as text))::text as "accesspolicy_id",
    'npu' as "access_policy_code"
from "dbt"."main_main"."GWAS_stg_subjectconsent" as subjectconsent
    where subjectconsent.consent in ('3')
        )
    union all(
    select DISTINCT 
    'ap' || '_' || md5('phs001584' || '|' || cast(coalesce(subjectconsent.consent, '') as text))::text as "accesspolicy_id",
    'pub' as "access_policy_code"
from "dbt"."main_main"."GWAS_stg_subjectconsent" as subjectconsent
    where subjectconsent.consent in ('2', 
          '8', 
          '9')
        )
    union all(
    select DISTINCT 
    'ap' || '_' || md5('phs001584' || '|' || cast(coalesce(subjectconsent.consent, '') as text))::text as "accesspolicy_id",
    'hmb' as "access_policy_code"
from "dbt"."main_main"."GWAS_stg_subjectconsent" as subjectconsent
    where subjectconsent.consent in ('4', 
          '5', 
          '8', 
          '10')
        )
    union all(
    select DISTINCT 
    'ap' || '_' || md5('phs001584' || '|' || cast(coalesce(subjectconsent.consent, '') as text))::text as "accesspolicy_id",
    'gso' as "access_policy_code"
from "dbt"."main_main"."GWAS_stg_subjectconsent" as subjectconsent
    where subjectconsent.consent in ('2', 
          '8', 
          '10')
        )
    union all(
    select DISTINCT 
    'ap' || '_' || md5('phs001584' || '|' || cast(coalesce(subjectconsent.consent, '') as text))::text as "accesspolicy_id",
    'mds' as "access_policy_code"
from "dbt"."main_main"."GWAS_stg_subjectconsent" as subjectconsent
    where subjectconsent.consent in ('5')
        )
    union all(
    select DISTINCT 
    'ap' || '_' || md5('phs001584' || '|' || cast(coalesce(subjectconsent.consent, '') as text))::text as "accesspolicy_id",
    'ds' as "access_policy_code"
from "dbt"."main_main"."GWAS_stg_subjectconsent" as subjectconsent
    where subjectconsent.consent in ('6', 
          '7')
        )
    
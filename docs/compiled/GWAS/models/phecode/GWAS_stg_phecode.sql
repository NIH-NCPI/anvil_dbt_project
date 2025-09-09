

with source as (
    select 
      "subject_id"::text as "subject_id",
        REPLACE(CAST("age_at_observation" AS VARCHAR(50)), 'NA', NULL)::text as "age_at_observation",
       "icd_code"::text as "icd_code",
       "icd_flag"::text as "icd_flag",
       "phecode"::text as "phecode"
    from "dbt"."main"."GWAS_Phecode_DS_20201027"
)

select 
  ROW_NUMBER() OVER () AS ftd_index,
  source.*
from source
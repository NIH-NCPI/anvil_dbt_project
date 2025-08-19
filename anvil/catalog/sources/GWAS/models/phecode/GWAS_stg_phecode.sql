{{ config(materialized='table') }}

with source as (
    select 
      "subject_id"::text as "subject_id",
       "age_at_observation"::text as "age_at_observation",
       "icd_code"::text as "icd_code",
       "icd_flag"::text as "icd_flag",
       "phecode"::text as "phecode"
    from {{ source('GWAS','GWAS_Phecode_DS_20201027') }}
)

select 
  ROW_NUMBER() OVER () AS ftd_index,
  source.*
from source

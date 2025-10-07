{{ config(materialized='table') }}

with source as (
    select 
      "subject_id"::text as "subject_id",
      "age_at_observation"::text as "age_at_observation",
      "icd_code"::text as "icd_code",
      "icd_flag"::text as "icd_flag",
      "phecode"::text as "phecode"
    FROM read_csv('data/GWAS/GWAS_Phecode_DS_20201027.csv',
    delim = ',',
    header = true,
    nullstr = ['NA', '.'],
    columns = {
        'subject_id': 'STRING',
        'age_at_observation': 'STRING',
        'icd_code': 'STRING',
        'icd_flag': 'STRING',
        'phecode': 'STRING',
    })
)

select 
  ROW_NUMBER() OVER () AS ftd_index,
  source.*
from source
{{ config(materialized='table') }}

with source as (
    select 
      "subject_id"::text as "subject_id",
       "decade_birth"::text as "decade_birth",
       "year_birth"::text as "year_birth",
       "sex"::text as "sex",
       "race"::text as "race",
       "ethnicity"::text as "ethnicity"
    from {{ source('GWAS','GWAS_Demographics_DS_20201109') }}
)

select 
  ROW_NUMBER() OVER () AS ftd_index,
  source.*
from source

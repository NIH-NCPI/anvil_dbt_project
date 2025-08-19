{{ config(materialized='table') }}

with source as (
    select 
      "family_id"::integer as "family_id",
       "subject_id"::integer as "subject_id",
       "mother"::integer as "mother",
       "father"::integer as "father",
       "sex"::text as "sex",
       "mz_twin_id"::integer as "mz_twin_id"
    from {{ source('GWAS','GWAS_Pedigree_DS_20200512') }}
)

select 
  ROW_NUMBER() OVER () AS ftd_index,
  source.*
from source

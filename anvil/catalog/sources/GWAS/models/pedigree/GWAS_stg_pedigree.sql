{{ config(materialized='table') }}

with source as (
    select 
      "family_id"::text as "family_id",
       "subject_id"::text as "subject_id",
        CASE WHEN mother = 'NA' THEN NULL
            ELSE mother
        END::text as "mother",
        CASE WHEN father = 'NA' THEN NULL
            ELSE father
        END::text as "father",
       "sex"::text as "sex",
       CASE WHEN mz_twin_id = 'NA' THEN NULL
            ELSE mz_twin_id
       END::text as "mz_twin_id",
    from {{ source('GWAS','GWAS_Pedigree_DS_20200512') }}
)

select 
  ROW_NUMBER() OVER () AS ftd_index,
  source.*
from source

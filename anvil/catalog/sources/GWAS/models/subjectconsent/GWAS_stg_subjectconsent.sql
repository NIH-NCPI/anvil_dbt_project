{{ config(materialized='table') }}

with source as (
    select 
      "subject_id"::text as "subject_id",
       "consent"::text as "consent",
       "subject_source"::text as "subject_source",
       "source_subject_id"::text as "source_subject_id"
    from {{ source('GWAS','GWAS_SubjectConsent_DS_20190813') }}
)

select 
  ROW_NUMBER() OVER () AS ftd_index,
  source.*
from source
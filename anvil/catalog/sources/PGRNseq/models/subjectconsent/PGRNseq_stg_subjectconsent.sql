{{ config(materialized='table') }}

with source as (
    select 
      "subject_id"::text as "subject_id",
       "consent"::text as "consent",
       "subject_source"::text as "subject_source",
       "source_subject_id"::text as "source_subject_id"
    from {{ source('PGRNseq','PGRNseq_SubjectConsent_DS_20170412') }}
)

select 
  ROW_NUMBER() OVER () AS ftd_index,
  source.*
from source

{{ config(materialized='table') }}

with source as (
    select 
      "subject_id"::text as "subject_id",
       "weight"::text as "weight",
       "height"::text as "height",
       "body_mass_index"::text as "body_mass_index",
       "age_observation"::text as "age_observation",
       "visit_number"::text as "visit_number"
    from {{ source('PGRNseq','PGRNseq_BMI_DS_2015') }}
)

select 
  ROW_NUMBER() OVER () AS ftd_index,
  source.*
from source

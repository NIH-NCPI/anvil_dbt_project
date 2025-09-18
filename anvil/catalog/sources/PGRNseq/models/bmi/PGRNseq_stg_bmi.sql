{{ config(materialized='table') }}

with source as (
    select 
      "subject_id"::text as "subject_id",
       REPLACE(CAST("weight" AS VARCHAR(50)), 'NA', NULL)::text as "weight",
       REPLACE(CAST("height" AS VARCHAR(50)), 'NA', NULL)::text as "height",
       REPLACE(CAST("body_mass_index" AS VARCHAR(50)), 'NA', NULL)::text as "body_mass_index",
       "age_observation"::text as "age_observation",
       "visit_number"::text as "visit_number"
    from {{ source('PGRNseq','PGRNseq_BMI_DS_2015') }}
)

select 
  ROW_NUMBER() OVER () AS ftd_index,
  source.*
from source

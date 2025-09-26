{{ config(materialized='table') }}

with source as (
    select 
      "subject_id"::text as "subject_id",
       "age_at_lipids"::text as "age_at_lipids",
       "on_statins_at_time_of_lab"::text as "on_statins_at_time_of_lab",
       "serum_total_cholesterol"::text as "serum_total_cholesterol",
       "ldl"::text as "ldl",
       "hdl"::text as "hdl",
       "triglyceride"::text as "triglyceride",
       "visit_number"::text as "visit_number"
    from {{ source('PGRNseq','PGRNseq_Labs_DS_2015') }}
)

select 
  ROW_NUMBER() OVER () AS ftd_index,
  source.*
from source

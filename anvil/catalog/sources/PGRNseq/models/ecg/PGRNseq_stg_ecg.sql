{{ config(materialized='table') }}

with source as (
    select 
      "subject_id"::text as "subject_id",
       "age_at_ecg"::text as "age_at_ecg",
       "ecg_qrs_duration"::text as "ecg_qrs_duration",
       "ecg_qt_interval"::text as "ecg_qt_interval",
       "ecg_qtc_bazett"::text as "ecg_qtc_bazett",
       "ecg_hr"::text as "ecg_hr",
       "ecg_pr_interval"::text as "ecg_pr_interval",
       "visit_number"::text as "visit_number"
    from {{ source('PGRNseq','PGRNseq_ECG_DS_2015') }}
)

select 
  ROW_NUMBER() OVER () AS ftd_index,
  source.*
from source

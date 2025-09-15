{{ config(materialized='table', schema='PGRNseq_data') }}

select 
  {{ generate_global_id(prefix='dm',descriptor=['demographics.subject_id'], study_id='phs000906') }}::text as "demographics_id",
  demographics.subject_id::text as "external_id"
from {{ ref('PGRNseq_stg_demographics') }} as demographics

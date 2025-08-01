{{ config(materialized='table', schema='cmg_bh_data') }}

select 
  {{ generate_global_id(prefix='dm',descriptor=['s.subject_id'], study_id='cmg_bh') }}::text as "demographics_id",
--   { { generate_global_id(prefix='',descriptor=[''], study_id='cmg_bh') }}::text as "source_data_id"
  NULL::text as "source_data_id" --TODO
from {{ ref('cmg_bh_stg_subject') }} as s

    
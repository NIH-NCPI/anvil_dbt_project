{{ config(materialized='table', schema='cmg_yale_data') }}

select 
--   { { generate_global_id(prefix='',descriptor=[''], study_id='cmg_yale') }}::text as "demographics_id",
--   { { generate_global_id(prefix='',descriptor=[''], study_id='cmg_yale') }}::text as "source_data_id"
  NULL::text as "demographics_id",
  NULL::text as "source_data_id"
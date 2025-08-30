{{ config(materialized='table', schema='cmg_yale_data') }}

select 
--   { { generate_global_id(prefix='',descriptor=[''], study_id='cmg_yale') }}::text as "sample_id",
  NULL::text as "sample_id",
  NULL::text as "storage_method"
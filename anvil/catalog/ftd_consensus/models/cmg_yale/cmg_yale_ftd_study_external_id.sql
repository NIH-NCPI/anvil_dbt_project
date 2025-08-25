{{ config(materialized='table', schema='cmg_yale_data') }}

select 
  {{ generate_global_id(prefix='st',descriptor=['phs000744'], study_id='cmg_yale') }}::text as "study_id",
  'phs000744'::text as "external_id"
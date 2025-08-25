{{ config(materialized='table', schema='cmg_yale_data') }}

select 
  NULL::text as "parent_study_id",
  'Yale Center for Mendelian Genomics (Y CMG)' as "study_title", 
  {{ generate_global_id(prefix='st',descriptor=['phs000744'], study_id='cmg_yale') }}::text as "id"
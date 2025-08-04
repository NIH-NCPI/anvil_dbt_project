{{ config(materialized='table', schema='cmg_bh_data') }}

select 
  NULL::text as "parent_study_id",
  'Baylor Hopkins Center for Mendelian Genomics (BH CMG)'::text as "study_title",
  {{ generate_global_id(prefix='sd',descriptor=['Baylor Hopkins Center for Mendelian Genomics (BH CMG)'], study_id='cmg_bh') }}::text as "id"
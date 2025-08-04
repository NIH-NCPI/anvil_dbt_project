{{ config(materialized='table', schema='cmg_bh_data') }}

select 
  {{ generate_global_id(prefix='sd',descriptor=['Baylor Hopkins Center for Mendelian Genomics (BH CMG)'], study_id='cmg_bh') }}::text as "id"
  NULL::text as "external_id"
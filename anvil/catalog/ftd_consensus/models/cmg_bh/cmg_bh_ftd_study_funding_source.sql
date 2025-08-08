{{ config(materialized='table', schema='cmg_bh_data') }}
with
lookup as (
select
  'Baylor Hopkins Center for Mendelian Genomics (BH CMG)' as "id"
)
select 
  {{ generate_global_id(prefix='sd',descriptor=['lookup.id'], study_id='cmg_bh') }}::text as "study_id",
  NULL::text as "funding_source"
from lookup

    
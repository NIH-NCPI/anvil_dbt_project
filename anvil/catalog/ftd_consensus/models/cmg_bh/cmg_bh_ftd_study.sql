{{ config(materialized='table', schema='cmg_bh_data') }}

with 
source as (
    select 
      {{ generate_global_id(prefix='sd',descriptor=['Baylor Hopkins Center for Mendelian Genomics (BH CMG)'], study_id='cmg_bh') }}::text as "id"
    from {{ ref('cmg_bh_stg_subject') }} as s
)

select 
  NULL::text as "parent_study_id",
  'Baylor Hopkins Center for Mendelian Genomics (BH CMG)'::text as "study_title",
  source.id
from source
    
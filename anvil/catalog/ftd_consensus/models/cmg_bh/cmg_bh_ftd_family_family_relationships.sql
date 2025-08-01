{{ config(materialized='table', schema='cmg_bh_data') }}

select 
  {{ generate_global_id(prefix='fy',descriptor=['family_id'], study_id='cmg_bh') }}::text as "family_id",
--   {{ generate_global_id(prefix='fr',descriptor=[''], study_id='cmg_bh') }}::text as "family_relationships_id"
  NULL::text as "family_relationships_id"
from {{ ref('cmg_bh_stg_subject') }} as subject
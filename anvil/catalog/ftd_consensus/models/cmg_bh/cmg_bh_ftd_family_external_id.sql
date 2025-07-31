{{ config(materialized='table', schema='cmg_bh_data') }}

with 
source as (
    select 
      {{ generate_global_id(prefix='fy',descriptor=['s.family_id'], study_id='cmg_bh') }}::text as "family_id",
      s.family_id::text as "external_id"
    from {{ ref('cmg_bh_stg_subject') }} as s
)

select 
  source.family_id,
  source.external_id
from source
    
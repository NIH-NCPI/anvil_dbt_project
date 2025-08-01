{{ config(materialized='table', schema='cmg_bh_data') }}

with
unique_ids as (
    select 
      {{ generate_global_id(prefix='dm',descriptor=['s.subject_id'], study_id='cmg_bh') }}::text as "demographics_id",
    from (select distinct subject_id from {{ ref('cmg_bh_stg_subject') }})
)

select
  demographics_id,
  NULL::text as "external_id"
from unique_ids
{{ config(materialized='table', schema='cmg_bh_data') }}

with
unique_ids as (
    select 
      {{ generate_global_id(prefix='',descriptor=[''], study_id='cmg_bh') }}::text as "familyrelationship_id",
    from (select distinct  from {{ ref('cmg_bh_stg_subject') }})
)

select
  NULL::text as "external_id"
from unique_ids
{{ config(materialized='table', schema='cmg_bh_data') }}

with
unique_ids as (
    select 
      {{ generate_global_id(prefix='bc',descriptor=['sample_id'], study_id='cmg_bh') }}::text as "biospecimencollection_id",
    from (select distinct sample_id from {{ ref('cmg_bh_stg_sample') }})
)

select
  biospecimencollection_id,
  NULL::text as "external_id"
from unique_ids
{{ config(materialized='table', schema='cmg_bh_data') }}

with
unique_ids as (
    select 
      {{ generate_global_id(prefix='fm',descriptor=['family_id','family_relationship'], study_id='cmg_bh') }}::text as "familymember_id",
    from (select distinct family_id, family_relationship, ingest_providence from {{ ref('cmg_bh_stg_subject') }})
)

select
  familymember_id,
  NULL::text as "external_id"
from unique_ids
{{ config(materialized='table', schema='cmg_bh_data') }}

with 
unique_ids as (
    select 
      distinct ingest_provenance
    from {{ ref('cmg_bh_stg_subject') }}  
)

select 
  {{ generate_global_id(prefix='ap',descriptor=['ingest_provenance'], study_id='cmg_bh') }}::text as "accesspolicy_id",
  NULL::text as "external_id"
from unique_ids
{{ config(materialized='table') }}

with source as (
    select 
      "family_id"::text as "family_id",
       "consanguinity"::text as "consanguinity",
       "ingest_provenance"::text as "ingest_provenance"
    from {{ source('cmg_yale','family') }}
)

select 
  ROW_NUMBER() OVER () AS ftd_index,
  source.*
from source

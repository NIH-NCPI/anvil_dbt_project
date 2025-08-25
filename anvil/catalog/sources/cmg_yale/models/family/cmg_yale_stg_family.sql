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
  source.*,
  REPLACE(ingest_provenance, 'family_', '') AS "consent_id",
from source
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
  REPLACE(REPLACE(UPPER(ingest_provenance), 'FAMILY_ANVIL_CMG_YALE_', ''),'.TSV','') AS "consent_id",
from source
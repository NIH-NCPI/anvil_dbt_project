{{ config(materialized='table') }}

with source as (
    select 
      "sample_id"::text as "sample_id",
       "sample_source"::text as "sample_source",
       "sequencing_center"::text as "sequencing_center",
       "subject_id"::text as "subject_id",
       "ingest_provenance"::text as "ingest_provenance",
       "subject_id2"::text as "subject_id2",
       "crai"::text as "crai",
       "cram"::text as "cram"
    from {{ source('cmg_yale','sample') }}
)

select 
  ROW_NUMBER() OVER () AS ftd_index,
  source.*,
  REPLACE(ingest_provenance, 'sample_', '') AS "consent_id",
from source

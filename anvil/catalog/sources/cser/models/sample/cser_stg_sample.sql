{{ config(materialized='table') }}

with source as (
    select 
      "sample_id"::text as "sample_id",
       "body_site"::text as "body_site",
       "dbgap_sample_id"::text as "dbgap_sample_id",
       "is_tumor"::text as "is_tumor",
       "primary_metastatic_tumor"::text as "primary_metastatic_tumor",
       "primary_tumor_location"::text as "primary_tumor_location",
       "sample_source"::text as "sample_source",
       "subject_id"::text as "subject_id",
       "subject_id_local"::text as "subject_id_local",
       "submitter_id"::text as "submitter_id",
       "tissue_affected_status"::text as "tissue_affected_status",
       "tumor_grade"::text as "tumor_grade",
       "tumor_stage"::text as "tumor_stage",
       "tumor_treatment"::text as "tumor_treatment",
       "ingest_provenance"::text as "ingest_provenance"
    from {{ source('cser','cser_sample') }}
)

select 
  ROW_NUMBER() OVER () AS ftd_index,
  source.*,
  REPLACE(REPLACE(UPPER(ingest_provenance), 'SAMPLE_ANVIL_CSER_SOUTHSEQ_', ''), '.TSV', '') as "consent_id"
from source

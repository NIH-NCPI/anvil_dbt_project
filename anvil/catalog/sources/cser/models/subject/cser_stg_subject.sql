{{ config(materialized='table') }}

with source as (
    select 
      "subject_id"::text as "subject_id",
       "dbgap_study_id"::text as "dbgap_study_id",
       "dbgap_submission"::text as "dbgap_submission",
       "disease_id"::text as "disease_id",
       "family_id"::text as "family_id",
       "hpo_absent"::text as "hpo_absent",
       "hpo_present"::text as "hpo_present",
       "maternal_id"::text as "maternal_id",
       "multiple_datasets"::text as "multiple_datasets",
       "paternal_id"::text as "paternal_id",
       "phenotype_group"::text as "phenotype_group",
       "pmids"::text as "pmids",
       "race_ethnicity"::text as "race_ethnicity",
       "sex"::text as "sex",
       "subject_id_local"::text as "subject_id_local",
       "ingest_provenance"::text as "ingest_provenance"
    from {{ source('cser','cser_subject') }}
)

select 
  ROW_NUMBER() OVER () AS ftd_index,
  source.*,
  REPLACE(REPLACE(UPPER(ingest_provenance), 'SUBJECT_ANVIL_CSER_SOUTHSEQ_', ''), '.TSV', '') as "consent_id"
from source

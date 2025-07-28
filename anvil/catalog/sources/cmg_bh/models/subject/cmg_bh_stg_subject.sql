{{ config(materialized='table') }}

    with source as (
        select 
        "subject_id"::text as "subject_id",
       "submission_batch"::text as "submission_batch",
       "affected_status"::text as "affected_status",
       "ancestry"::text as "ancestry",
       "dbgap_study_id"::text as "dbgap_study_id",
       "dbgap_submission"::text as "dbgap_submission",
       "family_id"::text as "family_id",
       "family_relationship"::text as "family_relationship",
       "hpo_present"::text as "hpo_present",
       "maternal_id"::text as "maternal_id",
       "multiple_datasets"::text as "multiple_datasets",
       "paternal_id"::text as "paternal_id",
       "phenotype_description"::text as "phenotype_description",
       "phenotype_group"::text as "phenotype_group",
       "project_investigator"::text as "project_investigator",
       "sex"::text as "sex",
       "solve_state"::text as "solve_state",
       "disease_id"::text as "disease_id",
       "ancestry_detail"::text as "ancestry_detail",
       "hpo_absent"::text as "hpo_absent",
       "twin_id"::text as "twin_id",
       "ingest_provenance"::text as "ingest_provenance"
        from {{ source('cmg_bh','subject') }}
    )

    select 
        ROW_NUMBER() OVER () AS ftd_index
        ,source.*
        from source
    
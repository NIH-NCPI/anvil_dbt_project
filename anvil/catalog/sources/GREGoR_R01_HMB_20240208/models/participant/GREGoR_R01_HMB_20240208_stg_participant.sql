{{ config(materialized='table') }}

    with source as (
        select 
        "participant_id"::text as "participant_id",
       "affected_status"::text as "affected_status",
       "age_at_enrollment"::text as "age_at_enrollment",
       "age_at_last_observation"::text as "age_at_last_observation",
       "ancestry_detail"::text as "ancestry_detail",
       "consent_code"::text as "consent_code",
       "family_id"::text as "family_id",
       "gregor_center"::text as "gregor_center",
       "internal_project_id"::text as "internal_project_id",
       "maternal_id"::text as "maternal_id",
       "paternal_id"::text as "paternal_id",
       "phenotype_description"::text as "phenotype_description",
       "pmid_id"::text as "pmid_id",
       "prior_testing"::text as "prior_testing",
       "proband_relationship"::text as "proband_relationship",
       "proband_relationship_detail"::text as "proband_relationship_detail",
       "recontactable"::text as "recontactable",
       "reported_ethnicity"::text as "reported_ethnicity",
       "reported_race"::text as "reported_race",
       "sex"::text as "sex",
       "sex_detail"::text as "sex_detail",
       "twin_id"::text as "twin_id",
       "ingest_provenance"::text as "ingest_provenance"
        from {{ source('GREGoR_R01_HMB_20240208','GREGoR_R01_HMB_20240208_participant') }}
    )

    select 
        *,
        concat('participant_id','-','family_id') as ftd_key
    from source
    
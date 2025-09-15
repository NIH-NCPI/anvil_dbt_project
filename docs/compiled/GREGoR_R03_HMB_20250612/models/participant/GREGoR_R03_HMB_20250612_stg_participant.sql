

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
       "missing_variant_case"::text as "missing_variant_case",
       "paternal_id"::text as "paternal_id",
       "phenotype_description"::text as "phenotype_description",
       "pmid_id"::text as "pmid_id",
       "prior_testing"::text as "prior_testing",
       "proband_relationship"::text as "proband_relationship",
       "proband_relationship_detail"::text as "proband_relationship_detail",
       "reported_ethnicity"::text as "reported_ethnicity",
       "reported_race"::text as "reported_race",
       "sex"::text as "sex",
       "sex_detail"::text as "sex_detail",
       "solve_status"::text as "solve_status",
       "twin_id"::text as "twin_id",
       "ingest_provenance"::text as "ingest_provenance"
        from "dbt"."main"."GREGoR_R03_HMB_20250612_participant"
    )

    select 
        ROW_NUMBER() OVER () AS ftd_index
        ,source.*
        from source
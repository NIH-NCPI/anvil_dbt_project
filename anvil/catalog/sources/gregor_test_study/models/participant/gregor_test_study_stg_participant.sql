{{ config(materialized='table') }}

    with source as (
        select 
        "AnVIL_GREGoR_GSS_U07_GRU_participant_id"::text AS "anvil_gregor_gss_u07_gru_participant_id",
       "affected_status"::text AS "affected_status",
       "age_at_enrollment"::text AS "age_at_enrollment",
       "age_at_last_observation"::text AS "age_at_last_observation",
       "ancestry_detail"::text AS "ancestry_detail",
       "consent_code"::text AS "consent_code",
       "family_id"::text AS "family_id",
       "gregor_center"::text AS "gregor_center",
       "internal_project_id"::text AS "internal_project_id",
       "maternal_id"::integer AS "maternal_id",
       "missing_variant_case"::text AS "missing_variant_case",
       "paternal_id"::integer AS "paternal_id",
       "phenotype_description"::text AS "phenotype_description",
       "pmid_id"::text AS "pmid_id",
       "prior_testing"::text AS "prior_testing",
       "Self_relationship"::text AS "self_relationship",
       "Self_relationship_detail"::text AS "self_relationship_detail",
       "reported_ethnicity"::text AS "reported_ethnicity",
       "reported_race"::text AS "reported_race",
       "sex"::text AS "sex",
       "sex_detail"::text AS "sex_detail",
       "solve_status"::text AS "solve_status",
       "twin_id"::text AS "twin_id"
        from {{ source('gregor_test_study','GREGoR_synthetic_participant_revised07Aug2024') }}
    )

    select 
        *,
        concat(study_code, '-', participant_global_id) as ftd_key
    from source
    
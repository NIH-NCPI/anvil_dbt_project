{{ config(materialized='table') }}

    with source as (
        select 
        "AnVIL_GREGoR_GSS_U07_GRU_phenotype_id"::text AS "anvil_gregor_gss_u07_gru_phenotype_id",
       "additional_details"::text AS "additional_details",
       "additional_modifiers"::text AS "additional_modifiers",
       "onset_age_range"::text AS "onset_age_range",
       "ontology"::text AS "ontology",
       "participant_id"::text AS "participant_id",
       "presence"::text AS "presence",
       "term_id"::text AS "term_id"
        from {{ source('gregor_test_study','GREGoR_synthetic_phenotype') }}
    )

    select 
        *,
        concat(study_code, '-', participant_global_id) as ftd_key
    from source
    
{{ config(materialized='table') }}

    with source as (
        select 
        "AnVIL_GREGoR_GSS_U07_GRU_phenotype_id"::text as "anvil_gregor_gss_u07_gru_phenotype_id",
       "additional_details"::text as "additional_details",
       "additional_modifiers"::text as "additional_modifiers",
       "onset_age_range"::text as "onset_age_range",
       "ontology"::text as "ontology",
       "participant_id"::text as "participant_id",
       "presence"::text as "presence",
       "term_id"::text as "term_id"
        from {{ source('gregor_synthetic','GREGoR_synthetic_phenotype') }}
    )

    select 
        *,
        concat('anvil_gregor_gss_u07_gru_phenotype_id','-','participant_id') as ftd_key
    from source
    
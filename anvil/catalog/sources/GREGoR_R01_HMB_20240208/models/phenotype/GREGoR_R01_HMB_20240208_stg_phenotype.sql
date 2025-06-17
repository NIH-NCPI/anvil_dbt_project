{{ config(materialized='table') }}

    with source as (
        select 
        "phenotype_id"::text as "phenotype_id",
       "additional_details"::text as "additional_details",
       "additional_modifiers"::text as "additional_modifiers",
       "onset_age_range"::text as "onset_age_range",
       "ontology"::text as "ontology",
       "participant_id"::text as "participant_id",
       "presence"::text as "presence",
       "term_id"::text as "term_id",
       "ingest_provenance"::text as "ingest_provenance"
        from {{ source('GREGoR_R01_HMB_20240208','GREGoR_R01_HMB_20240208_phenotype') }}
    )

    select 
        *,
        concat('phenotype_id','-','participant_id') as ftd_key
    from source
    
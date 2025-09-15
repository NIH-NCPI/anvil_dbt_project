

    with source as (
        select 
        "phenotype_id"::text as "phenotype_id",
       "additional_details"::text as "additional_details",
       "additional_modifiers"::text as "additional_modifiers",
       "onset_age_range"::text as "onset_age_range",
       "ontology"::text as "ontology",
       "participant_id"::text as "participant_id",
       "presence"::text as "presence",
       "syndromic"::text as "syndromic",
       "term_id"::text as "term_id"
        from "dbt"."main"."GREGoR_R03_GRU_20250612_phenotype"
    )

    select 
        ROW_NUMBER() OVER () AS ftd_index
        ,source.*
        from source
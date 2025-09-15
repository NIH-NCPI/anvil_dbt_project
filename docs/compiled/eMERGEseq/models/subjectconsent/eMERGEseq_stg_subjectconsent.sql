

    with source as (
        select 
        "subject_id"::text as "subject_id",
       "consent"::text as "consent",
       "subject_source"::text as "subject_source"
        from "dbt"."main"."eMERGEseq_SubjectConsent_DS_20201020"
    )

    select 
        ROW_NUMBER() OVER () AS ftd_index
        ,source.*
        from source
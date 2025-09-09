

    with source as (
        select 
        CASE WHEN mother = 'NA' THEN NULL
            ELSE mother
        END as "mother",
        CASE WHEN father = 'NA' THEN NULL
            ELSE father
        END as "father",
        CASE WHEN mz_twin_id = 'NA' THEN NULL
            ELSE mz_twin_id
        END as "mz_twin_id",
        "family_id"::text as "family_id",
       "subject_id"::text as "subject_id",
       "sex"::text as "sex",
        from "dbt"."main"."eMERGEseq_Pedigree_DS_20200925"
    )

    select 
        ROW_NUMBER() OVER () AS ftd_index
        ,source.*
        from source
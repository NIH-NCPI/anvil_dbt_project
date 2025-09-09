

    with source as (
        select 
        "subject_id"::text as "subject_id",
       "sample_id"::text as "sample_id",
       "sample_source"::text as "sample_source"
        from "dbt"."main"."eMERGEseq_SampleSubjectMapping_DS_20200925"
    )

    select 
        ROW_NUMBER() OVER () AS ftd_index
        ,source.*
        from source
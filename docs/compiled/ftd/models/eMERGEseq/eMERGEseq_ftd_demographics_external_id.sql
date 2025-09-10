

    with source as (
        select 
        'dm' || '_' || md5('phs001616' || '|' || cast(coalesce(demographics.subject_id, '') as text))::text as "demographics_id",
        demographics.subject_id::text as "external_id"
        from "dbt"."main_main"."eMERGEseq_stg_demographics" as demographics
    )

    select 
        * 
    from source
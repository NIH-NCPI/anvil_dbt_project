

    with source as (
        select 
        'sb' || '_' || md5('phs001616' || '|' || cast(coalesce(demographics.subject_id, '') as text))::text as "subject_id",
       demographics.subject_id::text as "external_id"
        from"dbt"."main_main"."eMERGEseq_stg_demographics" as demographics
    )

    select 
        * 
    from source
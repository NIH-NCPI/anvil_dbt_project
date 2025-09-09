

    with source as (
        select 
        'fm' || '_' || md5('phs001584' || '|' || cast(coalesce(subject_id, '') as text) || '|' || 'phs001584' || '|' || cast(coalesce(family_id, '') as text))::text as "familymember_id",
        subject_id::text as "external_id"
        from "dbt"."main_main"."GWAS_stg_pedigree" as pedigree
    )

    select 
        * 
    from source
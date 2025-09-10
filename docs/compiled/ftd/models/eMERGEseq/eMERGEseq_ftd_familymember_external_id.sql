

    with source as (
        select 
        'fm' || '_' || md5('phs001616' || '|' || cast(coalesce(pedigree.subject_id, '') as text) || '|' || 'phs001616' || '|' || cast(coalesce(pedigree.family_id, '') as text))::text as "familymember_id",
       pedigree.subject_id::text as "external_id"
        from "dbt"."main_main"."eMERGEseq_stg_pedigree" as pedigree
    )

    select 
        * 
    from source
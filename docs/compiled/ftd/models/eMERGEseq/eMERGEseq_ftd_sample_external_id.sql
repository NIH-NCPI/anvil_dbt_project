

    with source as (
        select 
        'sm' || '_' || md5('phs001616' || '|' || cast(coalesce(samplesubjectmapping.subject_id, '') as text) || '|' || 'phs001616' || '|' || cast(coalesce(samplesubjectmapping.sample_id, '') as text))::text as "sample_id",
       samplesubjectmapping.sample_id::text as "external_id"
        from "dbt"."main_main"."eMERGEseq_stg_samplesubjectmapping" as samplesubjectmapping
    )

    select 
        * 
    from source
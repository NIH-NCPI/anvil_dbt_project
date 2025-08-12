

    with source as (
        select 
        '' || '_' || md5('cmg_bh' || '|' || cast(coalesce(, '') as text))::text as "filemetadata_id",
       GEN_UNKNOWN.external_id::text as "external_id"
        from "dbt"."main_main"."cmg_bh_stg_sample" as sample
        join "dbt"."main_main"."cmg_bh_stg_subject" as subject
on sample.subject_id = subject.subject_id 
    )

    select 
        * 
    from source
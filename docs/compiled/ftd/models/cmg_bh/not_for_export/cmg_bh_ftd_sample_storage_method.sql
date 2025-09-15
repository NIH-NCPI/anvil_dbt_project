

    with source as (
        select 
        '' || '_' || md5('cmg_bh' || '|' || cast(coalesce(, '') as text))::text as "sample_id",
       GEN_UNKNOWN.storage_method::text as "storage_method"
        from "dbt"."main_main"."cmg_bh_stg_sample" as sample
        join "dbt"."main_main"."cmg_bh_stg_subject" as subject
on sample.subject_id = subject.subject_id 
    )

    select 
        * 
    from source
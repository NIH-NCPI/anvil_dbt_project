

    with source as (
        select 
        GEN_UNKNOWN.availablity_status::text as "availablity_status",
       GEN_UNKNOWN.quantity_number::text as "quantity_number",
       GEN_UNKNOWN.quantity_units::text as "quantity_units",
       GEN_UNKNOWN.concentration_number::text as "concentration_number",
       GEN_UNKNOWN.concentration_unit::text as "concentration_unit",
       '' || '_' || md5('cmg_bh' || '|' || cast(coalesce(, '') as text))::text as "has_access_policy",
       '' || '_' || md5('cmg_bh' || '|' || cast(coalesce(, '') as text))::text as "id",
       '' || '_' || md5('cmg_bh' || '|' || cast(coalesce(, '') as text))::text as "sample_id"
        from "dbt"."main_main"."cmg_bh_stg_sample" as sample
        join "dbt"."main_main"."cmg_bh_stg_subject" as subject
on sample.subject_id = subject.subject_id 
    )

    select 
        * 
    from source
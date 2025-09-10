

    with source as (
        select 
        'st' || '_' || md5('phs003047' || '|' || cast(coalesce(anvil_project.registered_identifier, '') as text))::text as "study_id",
       anvil_project.registered_identifier::text as "external_id"
        from "dbt"."main_main"."GREGoR_R03_GRU_20250612_stg_anvil_project" as anvil_project
    )

    select 
        * 
    from source
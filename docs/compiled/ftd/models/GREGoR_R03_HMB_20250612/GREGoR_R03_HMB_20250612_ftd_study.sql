

    with source as (
        select DISTINCT
        NULL as "parent_study_id",
       anvil_project.title::text as "study_title",
       'st' || '_' || md5('phs003047' || '|' || cast(coalesce(anvil_project.registered_identifier, '') as text))::text as "id"
        from "dbt"."main_main"."GREGoR_R03_HMB_20250612_stg_anvil_project" as anvil_project
    )

    select 
        * 
    from source
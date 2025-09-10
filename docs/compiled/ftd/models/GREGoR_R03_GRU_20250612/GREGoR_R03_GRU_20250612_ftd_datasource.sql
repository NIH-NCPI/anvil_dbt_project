

    with source as (
        select 
        GEN_UNKNOWN.snapshot_id::text as "snapshot_id",
       GEN_UNKNOWN.google_data_project::text as "google_data_project",
       GEN_UNKNOWN.snapshot_dataset::text as "snapshot_dataset",
       GEN_UNKNOWN.table::text as "table",
       GEN_UNKNOWN.parameterized_query::text as "parameterized_query",
       '' || '_' || md5('GREGoR_R03_GRU_20250612' || '|' || cast(coalesce(, '') as text))::text as "id"
        from "dbt"."main_main"."GREGoR_R03_GRU_20250612_stg_participant" as participant
        join "dbt"."main_main"."GREGoR_R03_GRU_20250612_stg_phenotype" as phenotype
on participant.participant_id = phenotype.participant_id  join "dbt"."main_main"."GREGoR_R03_GRU_20250612_stg_experiment" as experiment
on   join "dbt"."main_main"."GREGoR_R03_GRU_20250612_stg_anvil_project" as anvil_project
on  
    )

    select 
        * 
    from source
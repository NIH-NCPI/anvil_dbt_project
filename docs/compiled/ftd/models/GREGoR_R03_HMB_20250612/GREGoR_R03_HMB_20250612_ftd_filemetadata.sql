

    with source as (
        select 
        GEN_UNKNOWN.code::text as "code",
       GEN_UNKNOWN.display::text as "display",
       GEN_UNKNOWN.value_code::text as "value_code",
       GEN_UNKNOWN.value_display::text as "value_display",
       '' || '_' || md5('GREGoR_R03_HMB_20250612' || '|' || cast(coalesce(, '') as text))::text as "id"
        from "dbt"."main_main"."GREGoR_R03_HMB_20250612_stg_participant" as participant
        join "dbt"."main_main"."GREGoR_R03_HMB_20250612_stg_phenotype" as phenotype
on participant.participant_id = phenotype.participant_id 
    )

    select 
        * 
    from source
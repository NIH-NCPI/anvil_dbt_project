

    with source as (
        select 
        '' || '_' || md5('gregor_synthetic' || '|' || cast(coalesce(, '') as text))::text as "file_id",
       GEN_UNKNOWN.external_id::text as "external_id"
        from "dbt"."main_main"."gregor_synthetic_stg_participant" as participant
        join "dbt"."main_main"."gregor_synthetic_stg_phenotype" as phenotype
on participant.participant_id = phenotype.participant_id 
    )

    select 
        * 
    from source
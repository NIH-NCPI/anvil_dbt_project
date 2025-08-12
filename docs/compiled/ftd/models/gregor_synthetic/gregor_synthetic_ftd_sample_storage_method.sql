

    with source as (
        select 
        '' || '_' || md5('gregor_synthetic' || '|' || cast(coalesce(, '') as text))::text as "sample_id",
       GEN_UNKNOWN.storage_method::text as "storage_method"
        from "dbt"."main_main"."gregor_synthetic_stg_participant" as participant
        join "dbt"."main_main"."gregor_synthetic_stg_phenotype" as phenotype
on participant.participant_id = phenotype.participant_id 
    )

    select 
        * 
    from source
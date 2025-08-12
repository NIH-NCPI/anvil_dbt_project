

    with source as (
        select 
        'dm' || '_' || md5('gregor_synthetic' || '|' || cast(coalesce(participant.participant_id, '') as text))::text as "Demographics_id",
    --    GEN_UNKNOWN.external_id::text as "external_id"
       from "dbt"."main_main"."gregor_synthetic_stg_participant" as participant
        join "dbt"."main_main"."gregor_synthetic_stg_phenotype" as phenotype
on participant.participant_id = phenotype.participant_id 
    )

    select 
        * 
    from source
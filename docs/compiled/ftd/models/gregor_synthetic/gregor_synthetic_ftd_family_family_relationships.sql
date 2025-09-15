

    with source as (
        select 
        '' || '_' || md5('gregor_synthetic' || '|' || cast(coalesce(, '') as text))::text as "family_id",
       '' || '_' || md5('gregor_synthetic' || '|' || cast(coalesce(, '') as text))::text as "family_relationships_id"
        from "dbt"."main_main"."gregor_synthetic_stg_participant" as participant
        join "dbt"."main_main"."gregor_synthetic_stg_phenotype" as phenotype
on participant.participant_id = phenotype.participant_id 
    )

    select 
        * 
    from source
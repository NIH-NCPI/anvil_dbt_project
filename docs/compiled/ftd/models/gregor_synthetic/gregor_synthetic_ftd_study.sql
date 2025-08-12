

    with source as (
        select 
        '' || '_' || md5('gregor_synthetic' || '|' || cast(coalesce(, '') as text))::text as "parent_study_id",
       GEN_UNKNOWN.study_title::text as "study_title",
       '' || '_' || md5('gregor_synthetic' || '|' || cast(coalesce(, '') as text))::text as "id"
        from "dbt"."main_main"."gregor_synthetic_stg_participant" as participant
        join "dbt"."main_main"."gregor_synthetic_stg_phenotype" as phenotype
on participant.participant_id = phenotype.participant_id 
    )

    select 
        * 
    from source
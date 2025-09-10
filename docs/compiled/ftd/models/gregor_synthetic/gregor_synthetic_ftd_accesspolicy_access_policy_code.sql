

    with source as (
        select 
        'ap' || '_' || md5('gregor_synthetic' || '|' || cast(coalesce(participant.consent_code, '') as text))::text as "accesspolicy_id",
        participant.consent_code::text as "access_policy_code"
        from "dbt"."main_main"."gregor_synthetic_stg_participant" as participant
        join "dbt"."main_main"."gregor_synthetic_stg_phenotype" as phenotype
on participant.participant_id = phenotype.participant_id 
    )

    select 
        * 
    from source
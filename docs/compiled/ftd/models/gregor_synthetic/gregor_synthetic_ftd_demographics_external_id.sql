

    with source as (
        select distinct
        'dm' || '_' || md5('gregor_synthetic' || '|' || cast(coalesce(participant.participant_id, '') as text) || '|' || 'gregor_synthetic' || '|' || cast(coalesce(participant.consent_code, '') as text))::text as "Demographics_id",
       participant.participant_id::text as "external_id"
       from "dbt"."main_main"."gregor_synthetic_stg_participant" as participant
    )

    select 
        * 
    from source
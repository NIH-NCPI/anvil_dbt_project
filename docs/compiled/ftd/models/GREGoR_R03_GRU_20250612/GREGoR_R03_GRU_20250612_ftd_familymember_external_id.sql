

    with source as (
        select distinct
        'fm' || '_' || md5('GREGoR_R03_GRU_20250612' || '|' || cast(coalesce(participant.participant_id, '') as text))::text as "familyMember_id",
        participant.participant_id::text as "external_id"
        from "dbt"."main_main"."GREGoR_R03_GRU_20250612_stg_participant" as participant
    )

    select 
        * 
    from source
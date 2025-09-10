

    with source as (
        select DISTINCT
        'fm' || '_' || md5('phs003047' || '|' || cast(coalesce(participant.participant_id, participant.family_id, '') as text))::text as "familyMember_id",
        participant.participant_id::text as "external_id"
        from "dbt"."main_main"."GREGoR_R03_HMB_20250612_stg_participant" as participant
    )

    select 
        * 
    from source
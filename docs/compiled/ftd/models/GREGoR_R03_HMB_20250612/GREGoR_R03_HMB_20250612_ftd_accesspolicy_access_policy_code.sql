

    with source as (
        select DISTINCT
        'ap' || '_' || md5('phs003047' || '|' || cast(coalesce(participant.consent_code, '') as text))::text as "accesspolicy_id",
        LOWER(participant.consent_code)::text as "access_policy_code"
        from "dbt"."main_main"."GREGoR_R03_HMB_20250612_stg_participant" as participant
    )

    select 
        * 
    from source
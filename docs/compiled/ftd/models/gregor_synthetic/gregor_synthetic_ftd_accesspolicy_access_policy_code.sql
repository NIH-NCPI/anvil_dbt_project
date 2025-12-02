

    with source as (
        select distinct
        'ap' || '_' || md5('gregor_synthetic' || '|' || cast(coalesce(participant.consent_code, '') as text))::text as "accesspolicy_id",
        lower(participant.consent_code)::text as "access_policy_code"
        from "dbt"."main_main"."gregor_synthetic_stg_participant" as participant
    )

    select 
        * 
    from source


    with source as (
        select 
        'ap' || '_' || md5('GREGoR_R03_HMB_20250612' || '|' || cast(coalesce(participant.consent_code, '') as text))::text as "accesspolicy_id",
        LOWER(participant.consent_code)::text as "access_policy_code"
        from "dbt"."main_main"."GREGoR_R03_HMB_20250612_stg_participant" as participant
        join "dbt"."main_main"."GREGoR_R03_HMB_20250612_stg_phenotype" as phenotype
on participant.participant_id = phenotype.participant_id 
    )

    select 
        * 
    from source

    with source as (
        select DISTINCT
       CASE 
            WHEN experiment.participant_id IS NOT NULL then 'participant'
            ELSE 'non_participant'
        END::text as subject_type,
       NULL as "organism_type",
       'ap' || '_' || md5('phs003047' || '|' || cast(coalesce(participant.consent_code, '') as text))::text as "has_access_policy",
       'sb' || '_' || md5('phs003047' || '|' || cast(coalesce(participant.participant_id, '') as text))::text as "id",
       'dm' || '_' || md5('phs003047' || '|' || cast(coalesce(participant.participant_id, '') as text))::text as "has_demographics_id"
        from "dbt"."main_main"."GREGoR_R03_HMB_20250612_stg_participant" as participant
        join "dbt"."main_main"."GREGoR_R03_HMB_20250612_stg_experiment" as experiment
        on participant.participant_id = experiment.participant_id 
    )

    select 
        * 
    from source
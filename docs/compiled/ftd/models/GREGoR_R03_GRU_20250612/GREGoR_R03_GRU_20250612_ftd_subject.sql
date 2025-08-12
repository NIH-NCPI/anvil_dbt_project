
    with source as (
        select 
       CASE 
            WHEN experiment.participant_id IS NOT NULL then 'participant'
            ELSE 'non_participant'
        END::text as subject_type,
        --    GEN_UNKNOWN.organism_type::text as "organism_type",
       'ap' || '_' || md5('GREGoR_R03_GRU_20250612' || '|' || cast(coalesce(participant.consent_code, '') as text))::text as "has_access_policy",
       'sb' || '_' || md5('GREGoR_R03_GRU_20250612' || '|' || cast(coalesce(participant.participant_id, '') as text))::text as "id",
    
        from "dbt"."main_main"."GREGoR_R03_GRU_20250612_stg_participant" as participant
        join "dbt"."main_main"."GREGoR_R03_GRU_20250612_stg_experiment" as experiment
        on participant.participant_id = experiment.participant_id 
    )

    select 
        * 
    from source
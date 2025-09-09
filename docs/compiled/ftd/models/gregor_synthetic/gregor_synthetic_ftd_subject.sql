
    with source as (
        select 
        -- GEN_UNKNOWN.subject_type::text as "subject_type",
    --    GEN_UNKNOWN.organism_type::text as "organism_type",
       'ap' || '_' || md5('gregor_synthetic' || '|' || cast(coalesce(participant.consent_code, '') as text))::text as "has_access_policy",
       'sb' || '_' || md5('gregor_synthetic' || '|' || cast(coalesce(participant.participant_id, '') as text))::text as "id",
    
        from "dbt"."main_main"."gregor_synthetic_stg_participant" as participant
    )

    select 
        * 
    from source

    with source as (
        select distinct
        'participant'::text as "subject_type",
        'human'::text as "organism_type",
       'ap' || '_' || md5('gregor_synthetic' || '|' || cast(coalesce(participant.consent_code, '') as text))::text as "has_access_policy",
       'sb' || '_' || md5('gregor_synthetic' || '|' || cast(coalesce(participant.participant_id, '') as text) || '|' || 'gregor_synthetic' || '|' || cast(coalesce(participant.consent_code, '') as text))::text as "id",
       'dm' || '_' || md5('gregor_synthetic' || '|' || cast(coalesce(participant.participant_id, '') as text) || '|' || 'gregor_synthetic' || '|' || cast(coalesce(participant.consent_code, '') as text))::text as "has_demographics_id"
        from "dbt"."main_main"."gregor_synthetic_stg_participant" as participant
    )

    select 
        * 
    from source
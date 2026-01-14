

    with source as (
        select distinct
        NULL::integer as "date_of_birth",
        NULL::text as "date_of_birth_type",
         CASE participant.sex
            WHEN 'Female' THEN 'female'
            WHEN 'Male' THEN 'male'
            WHEN 'Unknown' THEN 'unknown'
            WHEN 'Intersex' THEN 'intersex'
        END::text as "sex",
        participant.sex::text as "sex_display",
        CASE
            WHEN participant.reported_race IS NULL THEN 'unknown'
            ELSE participant.reported_race
        END::text as "race_display",
        CASE
            WHEN  participant.reported_ethnicity = 'Hispanic or Latino' THEN 'hispanic_or_latino'
            WHEN  participant.reported_ethnicity = 'Not Hispanic or Latino' THEN 'not_hispanic_or_latino'
            WHEN  participant.reported_ethnicity IS NULL THEN 'unknown'
        END::text as "ethnicity",
        CASE
            WHEN participant.reported_ethnicity IS NULL THEN 'unknown'
            ELSE participant.reported_ethnicity
        END::text as "ethnicity_display",
        participant.age_at_last_observation::integer as "age_at_last_vital_status",
       'ap' || '_' || md5('gregor_synthetic' || '|' || cast(coalesce(participant.consent_code, '') as text))::text as "has_access_policy",
       'dm' || '_' || md5('gregor_synthetic' || '|' || cast(coalesce(participant.participant_id, '') as text))::text as "id"
        from "dbt"."main_main"."gregor_synthetic_stg_participant" as participant
    )

    select 
        * 
    from source
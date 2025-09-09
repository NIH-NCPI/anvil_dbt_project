

    with source as (
        select DISTINCT
        NULL as "date_of_birth",
        NULL as "date_of_birth_type",
        CASE 
            WHEN participant.sex = 'Female' THEN 'female'
            WHEN participant.sex = 'Male' THEN 'male'
            WHEN participant.sex = 'Unknown' THEN 'unknown'
            WHEN participant.sex = 'Intersex' THEN 'intersex'
            WHEN participant.sex IS NULL THEN 'unknown'
            ELSE CONCAT('FTD_FLAG: unhandled sex: ',sex)
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
        NULL as "vital_status",
       'ap' || '_' || md5('phs003047' || '|' || cast(coalesce(participant.consent_code, '') as text))::text as "has_access_policy",
       'dm' || '_' || md5('phs003047' || '|' || cast(coalesce(participant.participant_id, '') as text))::text as "id"
        from "dbt"."main_main"."GREGoR_R03_GRU_20250612_stg_participant" as participant
    )

    select 
        * 
    from source
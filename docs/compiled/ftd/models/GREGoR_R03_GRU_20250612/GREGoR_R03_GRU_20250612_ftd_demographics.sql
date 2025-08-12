

    with source as (
        select DISTINCT
        -- GEN_UNKNOWN.date_of_birth::integer as "date_of_birth",
        -- GEN_UNKNOWN.date_of_birth_type::text as "date_of_birth_type",
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
        -- GEN_UNKNOWN.vital_status::text as "vital_status",
        -- NOTE! Currently, For the model to run successfully, the global_id rows must be able to create SQL, the args cannot be invalid, even when commented out. Use placeholders for now.
-- To see the generated sql, run `dbt compile --select {model}`
-- Brenda plans to make these run easier later.
       'ap' || '_' || md5('GREGoR_R03_GRU_20250612' || '|' || cast(coalesce(participant.consent_code, '') as text))::text as "has_access_policy",
       'dm' || '_' || md5('GREGoR_R03_GRU_20250612' || '|' || cast(coalesce(participant.participant_id, '') as text))::text as "id"
        from "dbt"."main_main"."GREGoR_R03_GRU_20250612_stg_participant" as participant
        join "dbt"."main_main"."GREGoR_R03_GRU_20250612_stg_phenotype" as phenotype
        on participant.participant_id = phenotype.participant_id 
    )

    select 
        * 
    from source
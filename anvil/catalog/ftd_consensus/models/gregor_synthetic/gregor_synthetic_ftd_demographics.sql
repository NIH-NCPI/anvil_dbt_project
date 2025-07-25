{{ config(materialized='table', schema='gregor_synthetic_data') }}

    with source as (
        select 
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
       {{ generate_global_id(prefix='ap',descriptor=['participant.consent_code'], study_id='gregor_synthetic') }}::text as "has_access_policy",
       {{ generate_global_id(prefix='dm',descriptor=['participant.participant_id'], study_id='gregor_synthetic') }}::text as "id"
        from {{ ref('gregor_synthetic_stg_participant') }} as participant
        join {{ ref('gregor_synthetic_stg_phenotype') }} as phenotype
        on participant.participant_id = phenotype.participant_id 
    )

    select 
        * 
    from source
    
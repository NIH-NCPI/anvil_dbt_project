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
            ELSE participant.sex 
        END::text as "sex",
        -- GEN_UNKNOWN.sex_display::text as "sex_display",
        participant.reported_race::text as "race_display",
        CASE participant.reported_ethnicity
            WHEN 'Hispanic or Latino' THEN 'hispanic_or_latino'
            WHEN 'Not Hispanic or Latino' THEN 'not_hispanic_or_latino'
            WHEN 'Unknown' THEN 'unknown'
            WHEN 'asked but unknown' THEN 'asked_but_unknown'
            ELSE participant.reported_ethnicity
        END::text as "ethnicity",
        -- GEN_UNKNOWN.ethnicity_display::text as "ethnicity_display",
        participant.age_at_last_observation::integer as "age_at_last_vital_status",
        -- GEN_UNKNOWN.vital_status::text as "vital_status",
        -- GEN_UNKNOWN.has_access_policy::text as "has_access_policy",
        {{ generate_global_id(prefix='sb',descriptor=['participant.AnVIL_GREGoR_GSS_U07_GRU_participant_id'], study_id='gregor_synthetic') }}::text as "id",
        from {{ ref('gregor_synthetic_stg_participant') }} as participant
        -- join {{ ref('gregor_synthetic_stg_phenotype') }} as phenotype using (ftd_key)
    )

    select 
        * 
    from source
    
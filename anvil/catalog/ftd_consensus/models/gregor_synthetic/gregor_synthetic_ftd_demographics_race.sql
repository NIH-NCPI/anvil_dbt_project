{{ config(materialized='table', schema='gregor_synthetic_data') }}

    with source as (
        select 
            {{ generate_global_id(prefix='dm',descriptor=['participant.participant_id'], study_id='gregor_synthetic') }}::text as "Demographics_id",
         CASE 
            WHEN UNNEST(SPLIT(participant.reported_race, '|')) = 'American Indian or Alaska Native' THEN 'american_indian_or_alaskan_native'
            WHEN UNNEST(SPLIT(participant.reported_race, '|')) = 'Asian' THEN 'asian'
            WHEN UNNEST(SPLIT(participant.reported_race, '|')) = 'Black or African American' THEN 'black_or_african_american'
            WHEN UNNEST(SPLIT(participant.reported_race, '|')) = 'Native Hawaiian or Other Pacific Islander' THEN 'native_hawaiian_or_pacific_islander'
            WHEN UNNEST(SPLIT(participant.reported_race, '|')) = 'White' THEN 'white'
            WHEN UNNEST(SPLIT(participant.reported_race, '|')) = 'Middle Eastern or North African' THEN 'other_race'
            WHEN UNNEST(SPLIT(participant.reported_race, '|')) = 'Other Race' THEN 'other_race'
            WHEN UNNEST(SPLIT(participant.reported_race, '|')) = 'Unknown' THEN 'unknown'
            WHEN UNNEST(SPLIT(participant.reported_race, '|')) = 'asked but unknown' THEN 'asked_but_unknown'
            WHEN STRING_SPLIT(participant.reported_race, '|') IS NULL THEN 'unknown'
        END::text as "race"
        from {{ ref('gregor_synthetic_stg_participant') }} as participant
        join {{ ref('gregor_synthetic_stg_phenotype') }} as phenotype
    on participant.participant_id = phenotype.participant_id 
    )

    select 
        * 
    from source
    


    with source as (
        select distinct
            'dm' || '_' || md5('gregor_synthetic' || '|' || cast(coalesce(participant.participant_id, '') as text) || '|' || 'gregor_synthetic' || '|' || cast(coalesce(participant.consent_code, '') as text))::text as "Demographics_id",
         CASE 
            WHEN UNNEST(SPLIT(participant.reported_race, '|')) = 'American Indian or Alaskan Native' THEN 'american_indian_or_alaskan_native'
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
        from "dbt"."main_main"."gregor_synthetic_stg_participant" as participant
    )

    select 
        * 
    from source
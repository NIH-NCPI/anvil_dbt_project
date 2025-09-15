

    with source as (
        select DISTINCT
        'dm' || '_' || md5('phs003047' || '|' || cast(coalesce(participant.participant_id, '') as text))::text as "Demographics_id",
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
        from "dbt"."main_main"."GREGoR_R03_GRU_20250612_stg_participant" as participant
    )

    select 
        * 
    from source
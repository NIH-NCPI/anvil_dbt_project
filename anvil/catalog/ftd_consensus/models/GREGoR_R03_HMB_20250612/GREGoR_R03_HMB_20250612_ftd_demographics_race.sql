{{ config(materialized='table', schema='GREGoR_R03_HMB_20250612_data') }}

    with source as (
        select 
        {{ generate_global_id(prefix='dm',descriptor=['participant.participant_id'], study_id='GREGoR_R03_HMB_20250612') }}::text as "Demographics_id",
        CASE participant.reported_race
            WHEN 'American Indian or Alaskan Native' THEN 'american_indian_or_alaskan_native'
            WHEN 'Asian' THEN 'asian'
            WHEN 'Black or African American' THEN 'black_or_african_american'
            WHEN 'Native Hawaiian or Other Pacific Islander' THEN 'native_hawaiian_or_pacific_islander'
            WHEN 'White' THEN 'white'
            WHEN 'Other Race' THEN 'other_race'
            WHEN 'Unknown' THEN 'unknown'
            WHEN 'asked but unknown' THEN 'asked_but_unknown'
            ELSE participant.reported_race
        END::text as "race"
        from {{ ref('GREGoR_R03_HMB_20250612_stg_participant') }} as participant
        join {{ ref('GREGoR_R03_HMB_20250612_stg_phenotype') }} as phenotype
    on participant.participant_id = phenotype.participant_id 
    )

    select 
        * 
    from source
    

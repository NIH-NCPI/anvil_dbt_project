{{ config(materialized='table', schema='gregor_synthetic_data') }}

    with source as (
        select 
            {{ generate_global_id(prefix='dm',descriptor=['participant.anvil_gregor_gss_u07_gru_participant_id'], study_id='gregor_synthetic') }}::text as "Demographics_id",
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
        from {{ ref('gregor_synthetic_stg_participant') }} as participant
        join {{ ref('gregor_synthetic_stg_phenotype') }} as phenotype
    on participant.anvil_gregor_gss_u07_gru_participant_id = phenotype.participant_id 
    )

    select 
        * 
    from source
    
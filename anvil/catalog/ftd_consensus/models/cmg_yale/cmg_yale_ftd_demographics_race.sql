{{ config(materialized='table', schema='cmg_yale_data') }}

select
  {{ generate_global_id(prefix='dm',descriptor=['subject_id'], study_id='cmg_yale') }}::text as "demographics_id",
    case 
    when ancestry in ('american_indian_or_alaskan_native',
                        'asian',
                        'black_or_african_american',
                        'native_hawaiian_or_pacific_islander',
                        'white',
                        'other_race',
                        'unknown',
                        'asked_but_unknown')then ancestry
    when ancestry = 'American Indian or Alaskan Native' then 'american_indian_or_alaskan_native'
    when ancestry = 'Asian' then 'asian'
    when ancestry = 'Black or African American' then 'black_or_african_american'
    when ancestry = 'Native Hawaiian or Pacific Islander' then 'native_hawaiian_or_pacific_islander'
    when ancestry = 'Native Hawaiian or Other Pacific Islander' then 'native_hawaiian_or_pacific_islander'
    when ancestry = 'White' then 'white'
    when ancestry = 'Other' then 'other_race'
    when ancestry = 'Unknown' then 'unknown'
    when ancestry = 'Asked but Unknown' then 'asked_but_unknown'
    when ancestry = 'Hispanic or Latino' then 'unknown'
    when ancestry is null then null -- Cannot assume the question was not asked
    else CONCAT('FTD_FLAG:unhandled race: ',ancestry)
  end as "race",
from (select distinct sex, ancestry, subject_id, ingest_provenance from {{ ref('cmg_yale_stg_subject') }}) as s

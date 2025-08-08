{{ config(materialized='table', schema='cmg_bh_data') }}

select 
  {{ generate_global_id(prefix='dm',descriptor=['subject_id'], study_id='cmg_bh') }}::text as "demographics_id",
    case 
    when s.ancestry in ('American Indian or Alaskan Native',
                        'Asian',
                        'Black or African American',
                        'Native Hawaiian or Pacific Islander',
                        'White',
                        'Other',
                        'Unknown',
                        'Asked but Unknown')
      then ancestry
    when ancestry is null
      then null
    else CONCAT('FTD_FLAG:unhandled race: ',ancestry)
   end as "race",
from (select distinct ancestry, subject_id from {{ ref('cmg_bh_stg_subject') }}) as s
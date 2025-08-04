{{ config(materialized='table', schema='cmg_bh_data') }}

select 
  {{ generate_global_id(prefix='dm',descriptor=['s.subject_id'], study_id='cmg_bh') }}::text as "demographics_id",
    case 
    when s.ancestry in ('American Indian or Alaskan Native',
                        'Asian',
                        'Black or African American',
                        'Native Hawaiian or Pacific Islander',
                        'White',
                        'Other',
                        'Unknown',
                        'Asked but Unknown')
      then s.ancestry
    when s.ancestry is null
      then null
    else null
   end as "race",
from {{ ref('cmg_bh_stg_subject') }} as s
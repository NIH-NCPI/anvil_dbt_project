{{ config(materialized='table', schema='cmg_uwash_data') }}

select 
  coalesce(race_code,'unknown') AS "race",
  
  {{ generate_global_id(prefix='dm',descriptor=['subject_id', 'consent_id'], study_id='phs000693') }}::text as "demographics_id"
from (select 
        distinct 
        ancestry,
        subject_id,
        consent_id,
        race.code as "race_code",
        race.display as "race_display"
     from {{ ref('cmg_uwash_stg_subject') }} as sub
     left join {{ ref('dm_race') }} as race
     on lower(sub.ancestry) = race.src_format      
     ) as s
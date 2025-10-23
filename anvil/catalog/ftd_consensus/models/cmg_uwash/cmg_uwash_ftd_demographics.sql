{{ config(materialized='table', schema='cmg_uwash_data') }}

select 
NULL::integer as "date_of_birth",
NULL::text as "date_of_birth_type",
  coalesce(sex_code,'unknown')::text as "sex",  
  coalesce(sex_display,'Unknown')::text as "sex_display",
  coalesce(race_display,'Unknown')::text as "race_display",
  'unknown'::text as "ethnicity",
  'Unknown'::text as "ethnicity_display",
  NULL::integer as "age_at_last_vital_status",
  NULL::text as "vital_status",
    {{ generate_global_id(prefix='ap',descriptor=['consent_id'], study_id='phs000693') }}::text as "has_access_policy",
    {{ generate_global_id(prefix='dm',descriptor=['subject_id', 'consent_id'], study_id='phs000693') }}::text as "id"
from (select 
        distinct 
        sex,
        ancestry,
        subject_id,
        consent_id,
        sex.code as "sex_code",
        sex.display as "sex_display",
        race.display as "race_display"
     from {{ ref('cmg_uwash_stg_subject') }} as sub
     left join {{ ref('dm_sex') }} as sex
     on lower(sub.sex) = sex.src_format
     left join {{ ref('dm_race') }} as race
     on lower(sub.ancestry) = race.src_format  
     ) as s


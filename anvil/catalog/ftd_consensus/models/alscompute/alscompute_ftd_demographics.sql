{{ config(materialized='table', schema='alscompute_data') }}

select 
  NULL::integer as "date_of_birth",
  NULL::text as "date_of_birth_type",
  COALESCE(ds.code, 'unknown')::text as "sex",
  COALESCE(ds.display, 'Unknown')::text as "sex_display",
  'Unknown'::text as "race_display",
  'unknown'::text as "ethnicity",
  'Unknown'::text as "ethnicity_display",
  CASE
      WHEN LOWER(age_at_death_years) IN ('90 or older', '>90') THEN '90'
      ELSE age_at_death_years
  END as "age_at_last_vital_status",
  CASE
      WHEN age_at_death_years IS NOT NULL THEN 'Dead'
      ELSE 'Alive'
  END::text as "vital_status",
    {{ generate_global_id(prefix='ap',descriptor=['consent_id'], study_id='alscompute') }}::text as "has_access_policy",
    {{ generate_global_id(prefix='dm',descriptor=['sample_id','consent_id'], study_id='alscompute') }}::text as "id"
from (select distinct reported_gender_text, age_at_death_years, consent_id, sample_id from {{ ref('alscompute_stg_sample') }}) as s
left join {{ ref('dm_sex') }} as ds
    on lower(s.reported_gender_text) = lower(ds.src_format)

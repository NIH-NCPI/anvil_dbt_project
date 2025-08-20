{{ config(materialized='table', schema='cmg_yale_data') }}

select 
  NULL::integer as "date_of_birth",
  NULL::text as "date_of_birth_type",
    case
    when sex = 'Female' then 'female'
    when sex = 'Male' then 'male'
    when sex = 'Unknown' then 'unknown'
    when sex = 'Intersex' then 'intersex'
    when sex is null then 'unknown'
    else CONCAT('FTD_FLAG:unhandled sex: ',sex)
  end::text as "sex",
    case 
    when sex in ('Female',
                 'Male',
                 'Unknown',
                 'Intersex') then sex
    when sex is null then 'Unknown'
    else CONCAT('FTD_FLAG:unhandled sex_display: ',sex)
  end as "sex_display",

    case
    when ancestry in ('American Indian or Alaskan Native',
                      'Asian',
                      'Black or African American',
                      'Native Hawaiian or Pacific Islander',
                      'White',
                      'Other',
                      'Unknown',
                      'Asked but Unknown') then ancestry
    when ancestry = 'Native Hawaiian or Other Pacific Islander'
      then 'Native Hawaiian or Pacific Islander'
    when ancestry = 'Hispanic or Latino'
      then 'Unknown'
    when ancestry is null then null -- Cannot assume the question was not asked
    else CONCAT('FTD_FLAG:unhandled race_display: ',ancestry)
  end as "race_display",
  'unknown'::text as "ethnicity",
  'Unknown'::text as "ethnicity_display",
  NULL::integer as "age_at_last_vital_status",
  'unspecified'::text as "vital_status",
   {{ generate_global_id(prefix='ap',descriptor=['ingest_provenance'], study_id='cmg_yale') }}::text as "has_access_policy",
   {{ generate_global_id(prefix='dm',descriptor=['subject_id'], study_id='cmg_yale') }}::text as "id"
from (select distinct sex, ancestry, subject_id, ingest_provenance from {{ ref('cmg_yale_stg_subject') }}) as s

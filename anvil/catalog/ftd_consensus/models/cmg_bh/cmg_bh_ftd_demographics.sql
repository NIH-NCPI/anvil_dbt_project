{{ config(materialized='table', schema='cmg_bh_data') }}

 with source as (
    select 
        case
        when  subject.sex = 'Female'
          then 'female'
        when  subject.sex = 'Male'
          then 'male'
        when  subject.sex = 'Unknown'
          then 'unknown'
        when  subject.sex = 'Intersex'
          then 'intersex'
        when  subject.sex is null
          then 'unknown'
        else null
    end as "sex",
        case
        when  subject.sex = 'Female'
          then 'Female'
        when  subject.sex = 'Male'
          then 'Male'
        when  subject.sex = 'Unknown'
          then 'Unknown'
        when  subject.sex = 'Intersex'
          then 'Intersex'
        when  subject.sex is null
          then 'Unknown'
        else null
    end as "sex_display",
        case
        when  subject.ancestry = 'American Indian or Alaskan Native'
          then 'American Indian or Alaskan Native'
        when  subject.ancestry = 'Asian'
          then 'Asian'
        when  subject.ancestry = 'Black or African American'
          then 'Black or African American'
        when  subject.ancestry = 'Native Hawaiian or Pacific Islander'
          then 'Native Hawaiian or Pacific Islander'
        when  subject.ancestry = 'White'
          then 'White'
        when  subject.ancestry = 'Other'
          then 'Other'
        when  subject.ancestry = 'Unknown'
          then 'Unknown'
        when  subject.ancestry = 'Asked but Unknown'
          then 'Asked but Unknown'
        when  subject.ancestry is null
          then null
        else null
    end as "race_display",
    {{ generate_global_id(prefix='ap',descriptor=['sb.subject_id'], study_id='cmg_bh') }}::text as "has_access_policy",
    {{ generate_global_id(prefix='dm',descriptor=['sb.subject_id'], study_id='cmg_bh') }}::text as "id"
    from {{ ref('cmg_bh_stg_subject') }} as sb
    )

select 
   NULL::integer as "date_of_birth",
   NULL::text as "date_of_birth_type",
   source.sex,
   source.sex_display,
   source.race_display,
   'unknown'::text as "ethnicity",
   'Unknown'::text as "ethnicity_display",
   NULL::integer as "age_at_last_vital_status",
   'unspecified'::text as vital_status,
   source.has_access_policy,
   source.id
from source
    
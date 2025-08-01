{{ config(materialized='table', schema='cmg_bh_data') }}

 with 
 source as (
    select 
        case
          when s.sex = 'Female' then 'female'
          when s.sex = 'Male' then 'male'
          when s.sex = 'Unknown' then 'unknown'
          when s.sex = 'Intersex' then 'intersex'
          when s.sex is null then 'unknown'
        else CONCAT('FTD_FLAG:unhandled sex: ',s.sex)
      end::text as "sex",
        case 
          when s.sex in ('Female',
                         'Male',
                         'Unknown',
                         'Intersex') then s.sex
          when s.sex is null then 'Unknown'
        else CONCAT('FTD_FLAG:unhandled sex_display: ',s.sex)
      end as "sex_display",
        case
          when s.ancestry in ('American Indian or Alaskan Native',
                              'Asian',
                              'Black or African American',
                              'Native Hawaiian or Pacific Islander',
                              'White',
                              'Other',
                              'Unknown',
                              'Asked but Unknown') then s.ancestry
          when s.ancestry is null then null
        else CONCAT('FTD_FLAG:unhandled race_display: ',s.ancestry)
      end as "race_display",
      {{ generate_global_id(prefix='ap',descriptor=['ingest_provenance'], study_id='cmg_bh') }}::text as "has_access_policy",
      {{ generate_global_id(prefix='dm',descriptor=['s.subject_id'], study_id='cmg_bh') }}::text as "id"
    from (select distinct sex, ancestry, subject_id, ingest_provenance from {{ ref('cmg_bh_stg_subject') }}) as s
)

select 
   NULL::integer as "date_of_birth",
   NULL::text as "date_of_birth_type",
   source.sex as "sex",
   source.sex_display as "sex_display",
   source.race_display as "race_display",
   'unknown'::text as "ethnicity",
   'Unknown'::text as "ethnicity_display",
   NULL::integer as "age_at_last_vital_status",
   'unspecified'::text as vital_status,
   source.has_access_policy,
   source.id
from source
    
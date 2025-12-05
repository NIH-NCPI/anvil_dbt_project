{{ config(materialized='table', schema='cser_data') }}

with split_race as (
    select
    subject_id,
    UNNEST(SPLIT(lower(race_ethnicity), '|')) as "src_race"
    from {{ ref('cser_stg_subject') }} as s
    )
    
select 
NULL::integer as "date_of_birth",
NULL::text as "date_of_birth_type",
ds.code::text as "sex",
ds.display::text as "sex_display",
COALESCE(dr.display, 'unknown')::text as "race_display",
CASE 
    WHEN sr.src_race = 'hispanic or latino' THEN 'hispanic_or_latino'
    WHEN sr.src_race = 'hispanic/latino' THEN 'hispanic_or_latino'
    ELSE 'unknown'
END::text as "ethnicity",
CASE 
    WHEN sr.src_race = 'hispanic or latino' THEN 'Hispanic or Latino'
    WHEN sr.src_race = 'hispanic/latino' THEN 'Hispanic or Latino'
    ELSE 'unknown'
END::text as "ethnicity_display",
NULL::integer as "age_at_last_vital_status",
NULL::text as "vital_status",
subject_id,
{{ generate_global_id(prefix='ap',descriptor=['s.consent_id'], study_id='cser') }}::text as "has_access_policy",
{{ generate_global_id(prefix='dm',descriptor=['subject_id', 'consent_id'], study_id='cser') }}::text as "id"
from {{ ref('cser_stg_subject') }} as s
left join split_race as sr
    using(subject_id) 
left join {{ ref('dm_race') }} as dr
    on lower(sr.src_race) = lower(dr.src_format)
left join {{ ref('dm_sex') }} as ds
    on lower(s.sex) = lower(ds.src_format)
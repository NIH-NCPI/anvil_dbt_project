{{ config(materialized='table', schema='alscompute_data') }}

with derived_sample_type as (
    select 
    CASE
        WHEN age_of_collection_years ~ '^[A-Za-z]+$'
        THEN age_of_collection_years
        ELSE 'unknown'
    END as sample_type,
    consent_id,
    sample_id
    from (select distinct age_of_collection_years, consent_id, sample_id from {{ ref('alscompute_stg_sample') }})
    )
    
select 
  NULL::text as "parent_sample",
  curie::text as "sample_type",
  NULL::text as "availablity_status",
  NULL::text as "quantity_number",
  NULL::text as "quantity_units",
  {{ generate_global_id(prefix='ap',descriptor=['consent_id'], study_id='alscompute') }}::text as "has_access_policy",
  {{ generate_global_id(prefix='sm',descriptor=['sample_id'], study_id='alscompute') }}::text as "id",
  NULL::text as "subject_id",
  {{ generate_global_id(prefix='bc',descriptor=['sample_id'], study_id='alscompute') }}::text as "biospecimen_collection_id"
from derived_sample_type as d
LEFT JOIN  {{ ref('sm_sample_type') }} as s
on  d.sample_type = s.src_format 
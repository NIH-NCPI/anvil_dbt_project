{{ config(materialized='table', schema='cmg_uwash_data') }}

with c_or_d as (
    select 
    subject_id,
  NULL::text as "age_at_assertion",
  NULL::text as "age_at_event",
  NULL::text as "age_at_resolution",
  NULL::text as "code",
  NULL::text as "display",
  NULL::text as "value_code",
  NULL::text as "value_display",
  NULL::text as "value_number",
  NULL::text as "value_units",
  NULL::text as "value_units_display",
      UNNEST(SPLIT(disease_id, '|')) as d_code,
      UNNEST(SPLIT(hpo_present, '|')) as c_code,
    from {{ ref('cmg_uwash_stg_subject') }}
 )
 
select 
  CASE 
    WHEN d_code IS NOT NULL THEN 'disease'
    WHEN c_code IS NOT NULL THEN 'phenotypic_feature'
    ELSE NULL
  END::text as "assertion_type",
  age_at_assertion,
  age_at_event,
  age_at_resolution,
  coalesce(d_code,c_code,null)::text as "code",
  display,
  value_code,
  value_display,
  value_number,
  value_units,
  value_units_display,
    {{ generate_global_id(prefix='ap',descriptor=['consent_id'], study_id='phs000693') }}::text as "has_access_policy",
    {{ generate_global_id(prefix='sa',descriptor=['subject_id', 'code'], study_id='phs000693') }}::text as "id",
    subject_id
--     { { generate_global_id(prefix='sb',descriptor=['subject_id', 'consent_id'], study_id='phs000693') }}::text as "subject_id"
from {{ ref('cmg_uwash_stg_subject') }} as s
left join c_or_d as cd
using(subject_id)
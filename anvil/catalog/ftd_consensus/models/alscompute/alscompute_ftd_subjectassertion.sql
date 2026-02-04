{{ config(materialized='table', schema='alscompute_data') }}

select 
'disease'::text as "assertion_type",
CASE
    WHEN age_of_diagnosis_years = '-9' THEN NULL
    ELSE age_of_diagnosis_years
END::text as "age_at_assertion",
CASE
    WHEN age_of_onset_years = '-9' THEN NULL
    ELSE age_of_onset_years
END::text as "age_at_event",
NULL::text as "age_at_resolution",
NULL::text as "code",
NULL::text as "display",
NULL::text as "value_code",
NULL::text as "value_display",
NULL::text as "value_number",
NULL::text as "value_units",
NULL::text as "value_units_display",
    {{ generate_global_id(prefix='ap',descriptor=['consent_id'], study_id='alscompute') }}::text as "has_access_policy",
    {{ generate_global_id(prefix='sa',descriptor=['sample_id','code'], study_id='alscompute') }}::text as "id",
    {{ generate_global_id(prefix='sb',descriptor=['sample_id','consent_id'], study_id='alscompute') }}::text as "subject_id"
from (select distinct age_of_diagnosis_years, age_of_onset_years, sample_id, consent_id from {{ ref('alscompute_stg_sample') }}) as sample
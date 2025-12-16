{{ config(materialized='table', schema='cser_data') }}


select 
'phenotypic_feature'::text as "assertion_type",
NULL::text as "age_at_assertion",
NULL::text as "age_at_event",
NULL::text as "age_at_resolution",
  GEN_UNKNOWN.code::text as "code",
  GEN_UNKNOWN.display::text as "display",
  GEN_UNKNOWN.value_code::text as "value_code",
  GEN_UNKNOWN.value_display::text as "value_display",
NULL::text as "value_number",
NULL::text as "value_units",
NULL::text as "value_units_display",
    {{ generate_global_id(prefix='ap',descriptor=['consent_id'], study_id='cser') }}::text as "has_access_policy",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='cser') }}::text as "id",
    {{ generate_global_id(prefix='sb',descriptor=['subject_id','consent_id'], study_id='cser') }}::text as "subject_id"
from {{ ref('cser_stg_subject') }} as s

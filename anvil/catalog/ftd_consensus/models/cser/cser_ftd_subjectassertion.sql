{{ config(materialized='table', schema='cser_data') }}

select 
GEN_UNKNOWN.assertion_type::text as "assertion_type",
  GEN_UNKNOWN.age_at_assertion::text as "age_at_assertion",
  GEN_UNKNOWN.age_at_event::text as "age_at_event",
  GEN_UNKNOWN.age_at_resolution::text as "age_at_resolution",
  GEN_UNKNOWN.code::text as "code",
  GEN_UNKNOWN.display::text as "display",
  GEN_UNKNOWN.value_code::text as "value_code",
  GEN_UNKNOWN.value_display::text as "value_display",
  GEN_UNKNOWN.value_number::text as "value_number",
  GEN_UNKNOWN.value_units::text as "value_units",
  GEN_UNKNOWN.value_units_display::text as "value_units_display",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='cser') }}::text as "has_access_policy",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='cser') }}::text as "id",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='cser') }}::text as "subject_id"
from {{ ref('cser_stg_file_inventory') }} as file_inventory
join {{ ref('cser_stg_sample') }} as sample
on subject.subject_id = sample.subject_id  join {{ ref('cser_stg_sequencing') }} as sequencing
on   join {{ ref('cser_stg_subject') }} as subject
on sample.subject_id = subject.subject_id 


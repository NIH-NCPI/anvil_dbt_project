{{ config(materialized='table', schema='cmg_yale_data') }}

select 
GEN_UNKNOWN.availablity_status::text as "availablity_status",
  GEN_UNKNOWN.quantity_number::text as "quantity_number",
  GEN_UNKNOWN.quantity_units::text as "quantity_units",
  GEN_UNKNOWN.concentration_number::text as "concentration_number",
  GEN_UNKNOWN.concentration_unit::text as "concentration_unit",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='cmg_yale') }}::text as "has_access_policy",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='cmg_yale') }}::text as "id",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='cmg_yale') }}::text as "sample_id"
from {{ ref('cmg_yale_stg_sample') }} as sample
join {{ ref('cmg_yale_stg_subject') }} as subject
on sample.subject_id = subject.subject_id 


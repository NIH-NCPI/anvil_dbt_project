{{ config(materialized='table', schema='cser_data') }}

select 
GEN_UNKNOWN.code::text as "code",
  GEN_UNKNOWN.display::text as "display",
  GEN_UNKNOWN.value_code::text as "value_code",
  GEN_UNKNOWN.value_display::text as "value_display",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='cser') }}::text as "id"
from {{ ref('cser_stg_file_inventory') }} as file_inventory
join {{ ref('cser_stg_sample') }} as sample
on subject.subject_id = sample.subject_id  join {{ ref('cser_stg_sequencing') }} as sequencing
on   join {{ ref('cser_stg_subject') }} as subject
on sample.subject_id = subject.subject_id 


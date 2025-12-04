{{ config(materialized='table', schema='cser_data') }}

select 
GEN_UNKNOWN.family_type::text as "family_type",
  GEN_UNKNOWN.family_description::text as "family_description",
  GEN_UNKNOWN.consanguinity::text as "consanguinity",
  GEN_UNKNOWN.family_study_focus::text as "family_study_focus",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='cser') }}::text as "has_access_policy",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='cser') }}::text as "id"
from {{ ref('cser_stg_file_inventory') }} as file_inventory
join {{ ref('cser_stg_sample') }} as sample
on subject.subject_id = sample.subject_id  join {{ ref('cser_stg_sequencing') }} as sequencing
on   join {{ ref('cser_stg_subject') }} as subject
on sample.subject_id = subject.subject_id 


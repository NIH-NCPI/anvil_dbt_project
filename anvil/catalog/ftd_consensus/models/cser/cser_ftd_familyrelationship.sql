{{ config(materialized='table', schema='cser_data') }}

select 
  {{ generate_global_id(prefix='',descriptor=[''], study_id='cser') }}::text as "family_member",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='cser') }}::text as "other_family_member",
  GEN_UNKNOWN.relationship_code::text as "relationship_code",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='cser') }}::text as "has_access_policy",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='cser') }}::text as "id"
from {{ ref('cser_stg_file_inventory') }} as file_inventory
join {{ ref('cser_stg_sample') }} as sample
on subject.subject_id = sample.subject_id  join {{ ref('cser_stg_sequencing') }} as sequencing
on   join {{ ref('cser_stg_subject') }} as subject
on sample.subject_id = subject.subject_id 


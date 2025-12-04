{{ config(materialized='table', schema='cser_data') }}

select 
  {{ generate_global_id(prefix='',descriptor=[''], study_id='cser') }}::text as "parent_sample",
  GEN_UNKNOWN.sample_type::text as "sample_type",
  GEN_UNKNOWN.availablity_status::text as "availablity_status",
  GEN_UNKNOWN.quantity_number::text as "quantity_number",
  GEN_UNKNOWN.quantity_units::text as "quantity_units",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='cser') }}::text as "has_access_policy",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='cser') }}::text as "id",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='cser') }}::text as "subject_id",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='cser') }}::text as "biospecimen_collection_id"
from {{ ref('cser_stg_file_inventory') }} as file_inventory
join {{ ref('cser_stg_sample') }} as sample
on subject.subject_id = sample.subject_id  join {{ ref('cser_stg_sequencing') }} as sequencing
on   join {{ ref('cser_stg_subject') }} as subject
on sample.subject_id = subject.subject_id 


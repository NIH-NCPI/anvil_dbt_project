{{ config(materialized='table', schema='cser_datasets_data') }}

select 
GEN_UNKNOWN.subject_type::text as "subject_type",
  GEN_UNKNOWN.organism_type::text as "organism_type",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='cser_datasets') }}::text as "has_access_policy",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='cser_datasets') }}::text as "id",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='cser_datasets') }}::text as "has_demographics_id"
from {{ ref('cser_datasets_stg_anvil_dataset') }} as anvil_dataset
join {{ ref('cser_datasets_stg_file_inventory') }} as file_inventory
on   join {{ ref('cser_datasets_stg_sample') }} as sample
on subject.subject_id = sample.subject_id  join {{ ref('cser_datasets_stg_sequencing') }} as sequencing
on   join {{ ref('cser_datasets_stg_subject') }} as subject
on sample.subject_id = subject.subject_id 


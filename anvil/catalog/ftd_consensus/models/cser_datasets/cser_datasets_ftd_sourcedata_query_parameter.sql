{{ config(materialized='table', schema='cser_datasets_data') }}

select 
  {{ generate_global_id(prefix='',descriptor=[''], study_id='cser_datasets') }}::text as "sourcedata_id",
  GEN_UNKNOWN.query_parameter::text as "query_parameter"
from {{ ref('cser_datasets_stg_anvil_dataset') }} as anvil_dataset
join {{ ref('cser_datasets_stg_file_inventory') }} as file_inventory
on   join {{ ref('cser_datasets_stg_sample') }} as sample
on subject.subject_id = sample.subject_id  join {{ ref('cser_datasets_stg_sequencing') }} as sequencing
on   join {{ ref('cser_datasets_stg_subject') }} as subject
on sample.subject_id = subject.subject_id 


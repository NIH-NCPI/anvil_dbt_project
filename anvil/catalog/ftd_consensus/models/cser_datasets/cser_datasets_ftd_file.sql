{{ config(materialized='table', schema='cser_datasets_data') }}

select 
GEN_UNKNOWN.filename::text as "filename",
  GEN_UNKNOWN.format::text as "format",
  GEN_UNKNOWN.data_type::text as "data_type",
  GEN_UNKNOWN.size::text as "size",
  GEN_UNKNOWN.drs_uri::text as "drs_uri",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='cser_datasets') }}::text as "file_metadata",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='cser_datasets') }}::text as "has_access_policy",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='cser_datasets') }}::text as "id"
from {{ ref('cser_datasets_stg_anvil_dataset') }} as anvil_dataset
join {{ ref('cser_datasets_stg_file_inventory') }} as file_inventory
on   join {{ ref('cser_datasets_stg_sample') }} as sample
on subject.subject_id = sample.subject_id  join {{ ref('cser_datasets_stg_sequencing') }} as sequencing
on   join {{ ref('cser_datasets_stg_subject') }} as subject
on sample.subject_id = subject.subject_id 


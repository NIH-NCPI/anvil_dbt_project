{{ config(materialized='table', schema='cser_data') }}

select 
GEN_UNKNOWN.snapshot_id::text as "snapshot_id",
  GEN_UNKNOWN.google_data_project::text as "google_data_project",
  GEN_UNKNOWN.snapshot_dataset::text as "snapshot_dataset",
  GEN_UNKNOWN.table_id::text as "table_id",
  GEN_UNKNOWN.parameterized_query::text as "parameterized_query",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='cser') }}::text as "id"
from {{ ref('cser_stg_file_inventory') }} as file_inventory
join {{ ref('cser_stg_sample') }} as sample
on subject.subject_id = sample.subject_id  join {{ ref('cser_stg_sequencing') }} as sequencing
on   join {{ ref('cser_stg_subject') }} as subject
on sample.subject_id = subject.subject_id 


{{ config(materialized='table', schema='cmg_uwash_data') }}

select 
GEN_UNKNOWN.snapshot_id::text as "snapshot_id",
  GEN_UNKNOWN.google_data_project::text as "google_data_project",
  GEN_UNKNOWN.snapshot_dataset::text as "snapshot_dataset",
  GEN_UNKNOWN.table_id::text as "table_id",
  GEN_UNKNOWN.parameterized_query::text as "parameterized_query",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='cmg_uwash') }}::text as "id"
from {{ ref('cmg_uwash_stg_sample') }} as sample
join {{ ref('cmg_uwash_stg_subject') }} as subject
on sample.subject_id = subject.subject_id  join {{ ref('cmg_uwash_stg_anvil_dataset') }} as anvil_dataset
on   join {{ ref('cmg_uwash_stg_sequencing') }} as sequencing
on   join {{ ref('cmg_uwash_stg_family') }} as family
on   join {{ ref('cmg_uwash_stg_file_inventory') }} as file_inventory
on  


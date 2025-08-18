{{ config(materialized='table', schema='cmg_yale_data') }}

select 
GEN_UNKNOWN.snapshot_id::text as "snapshot_id",
  GEN_UNKNOWN.google_data_project::text as "google_data_project",
  GEN_UNKNOWN.snapshot_dataset::text as "snapshot_dataset",
  GEN_UNKNOWN.table::text as "table",
  GEN_UNKNOWN.parameterized_query::text as "parameterized_query",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='cmg_yale') }}::text as "id"
from {{ ref('cmg_yale_stg_sample') }} as sample
join {{ ref('cmg_yale_stg_subject') }} as subject
on sample.subject_id = subject.subject_id 


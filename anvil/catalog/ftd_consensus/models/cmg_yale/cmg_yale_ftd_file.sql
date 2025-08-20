{{ config(materialized='table', schema='cmg_yale_data') }}

select 
GEN_UNKNOWN.filename::text as "filename",
  GEN_UNKNOWN.format::text as "format",
  GEN_UNKNOWN.data_type::text as "data_type",
  GEN_UNKNOWN.size::integer as "size",
  GEN_UNKNOWN.drs_uri::text as "drs_uri",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='cmg_yale') }}::text as "file_metadata",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='cmg_yale') }}::text as "has_access_policy",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='cmg_yale') }}::text as "id"
from {{ ref('cmg_yale_stg_sample') }} as sample
join {{ ref('cmg_yale_stg_subject') }} as subject
on sample.subject_id = subject.subject_id  join {{ ref('cmg_yale_stg_anvil_dataset') }} as anvil_dataset
on   join {{ ref('cmg_yale_stg_sequencing') }} as sequencing
on   join {{ ref('cmg_yale_stg_family') }} as family
on  


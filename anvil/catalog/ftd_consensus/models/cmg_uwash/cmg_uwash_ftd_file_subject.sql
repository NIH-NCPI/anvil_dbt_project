{{ config(materialized='table', schema='cmg_uwash_data') }}

select 
  {{ generate_global_id(prefix='fl',descriptor=['name'], study_id='phs000693') }}::text as "file_id",
    {{ generate_global_id(prefix='sb',descriptor=['subject_id'], study_id='phs000693') }}::text as "subject_id"
from {{ ref('cmg_uwash_stg_sample') }} as s
left join {{ ref('cmg_uwash_stg_file_inventory') }} as fi
on s.bam = fi.file_ref
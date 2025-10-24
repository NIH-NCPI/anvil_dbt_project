{{ config(materialized='table', schema='cmg_uwash_data') }}

select 
  {{ generate_global_id(prefix='fl',descriptor=['name'], study_id='phs000693') }}::text as "file_id",
    {{ generate_global_id(prefix='sm',descriptor=['subject_id', 'sample_id'], study_id='phs000693') }}::text as "sample_id"
from {{ ref('cmg_uwash_stg_sample') }} as s
left join {{ ref('cmg_uwash_stg_file_inventory') }} as fi
on s.bam = fi.file_ref
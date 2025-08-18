{{ config(materialized='table', schema='cmg_yale_data') }}

select 
  {{ generate_global_id(prefix='',descriptor=[''], study_id='cmg_yale') }}::text as "demographics_id",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='cmg_yale') }}::text as "source_data_id"
from {{ ref('cmg_yale_stg_sample') }} as sample
join {{ ref('cmg_yale_stg_subject') }} as subject
on sample.subject_id = subject.subject_id 


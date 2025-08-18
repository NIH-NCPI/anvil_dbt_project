{{ config(materialized='table', schema='cmg_yale_data') }}

select 
GEN_UNKNOWN.disease_limitation::text as "disease_limitation",
  GEN_UNKNOWN.description::text as "description",
  GEN_UNKNOWN.website::text as "website",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='cmg_yale') }}::text as "id"
from {{ ref('cmg_yale_stg_sample') }} as sample
join {{ ref('cmg_yale_stg_subject') }} as subject
on sample.subject_id = subject.subject_id 


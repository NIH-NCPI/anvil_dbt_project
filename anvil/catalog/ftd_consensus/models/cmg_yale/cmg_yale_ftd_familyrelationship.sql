{{ config(materialized='table', schema='cmg_yale_data') }}

select 
  {{ generate_global_id(prefix='',descriptor=[''], study_id='cmg_yale') }}::text as "family_member",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='cmg_yale') }}::text as "other_family_member",
  GEN_UNKNOWN.relationship_code::text as "relationship_code",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='cmg_yale') }}::text as "has_access_policy",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='cmg_yale') }}::text as "id"
from {{ ref('cmg_yale_stg_sample') }} as sample
join {{ ref('cmg_yale_stg_subject') }} as subject
on sample.subject_id = subject.subject_id 


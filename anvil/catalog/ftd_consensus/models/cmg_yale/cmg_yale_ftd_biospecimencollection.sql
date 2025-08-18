{{ config(materialized='table', schema='cmg_yale_data') }}

select 
GEN_UNKNOWN.age_at_collection::integer as "age_at_collection",
  GEN_UNKNOWN.method::text as "method",
  GEN_UNKNOWN.site::text as "site",
  GEN_UNKNOWN.spatial_qualifier::text as "spatial_qualifier",
  GEN_UNKNOWN.laterality::text as "laterality",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='cmg_yale') }}::text as "has_access_policy",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='cmg_yale') }}::text as "id"
from {{ ref('cmg_yale_stg_sample') }} as sample
join {{ ref('cmg_yale_stg_subject') }} as subject
on sample.subject_id = subject.subject_id 


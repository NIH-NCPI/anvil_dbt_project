{{ config(materialized='table', schema='cser_data') }}

select 
GEN_UNKNOWN.age_at_collection::integer as "age_at_collection",
  GEN_UNKNOWN.method::text as "method",
  GEN_UNKNOWN.site::text as "site",
  GEN_UNKNOWN.spatial_qualifier::text as "spatial_qualifier",
  GEN_UNKNOWN.laterality::text as "laterality",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='cser') }}::text as "has_access_policy",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='cser') }}::text as "id"
from {{ ref('cser_stg_file_inventory') }} as file_inventory
join {{ ref('cser_stg_sample') }} as sample
on subject.subject_id = sample.subject_id  join {{ ref('cser_stg_sequencing') }} as sequencing
on   join {{ ref('cser_stg_subject') }} as subject
on sample.subject_id = subject.subject_id 


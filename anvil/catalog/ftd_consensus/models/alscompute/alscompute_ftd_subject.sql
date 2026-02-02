{{ config(materialized='table', schema='alscompute_data') }}

select 
'participant'::text as "subject_type",
NULL::text as "organism_type",
    {{ generate_global_id(prefix='ap',descriptor=['consent_id'], study_id='alscompute') }}::text as "has_access_policy",
    {{ generate_global_id(prefix='sb',descriptor=['sample_id','consent_id'], study_id='alscompute') }}::text as "id",
    {{ generate_global_id(prefix='dm',descriptor=['sample_id','consent_id'], study_id='alscompute') }}::text as "has_demographics_id"
from (select distinct consent_id, sample_id from {{ ref('alscompute_stg_sample') }}) as sample
{{ config(materialized='table', schema='cmg_yale_data') }}

select 
  distinct
--   { { generate_global_id(prefix='sm',descriptor=['sample_id'], study_id='cmg_yale') }}::text as "parent_sample",
  NULL::text as "parent_sample",
  sample_source::text as "sample_type",
  NULL::text as "availablity_status",
  NULL::text as "quantity_number",
  NULL::text as "quantity_units",
  {{ generate_global_id(prefix='ap',descriptor=['consent_id'], study_id='cmg_yale') }}::text as "has_access_policy",
  {{ generate_global_id(prefix='sm',descriptor=['sample_id','subject_id'], study_id='cmg_yale') }}::text as "id",
  {{ generate_global_id(prefix='sb',descriptor=['subject_id'], study_id='cmg_yale') }}::text as "subject_id",
--   {  { generate_global_id(prefix='bc',descriptor=['sample_id','{TBD}'], study_id='cmg_yale') }}::text as "biospecimen_collection_id"
  NULL::text as "biospecimen_collection_id"
from (select distinct subject_id, consent_id, sample_source, sample_id 
      from {{ ref('cmg_yale_stg_sample') }}
      ) as sam
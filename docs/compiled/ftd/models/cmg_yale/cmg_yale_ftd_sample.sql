

select 
  distinct
--   { { generate_global_id(prefix='sm',descriptor=['sample_id'], study_id='cmg_yale') }}::text as "parent_sample",
  NULL::text as "parent_sample",
  
  curie AS "sample_type",
  coalesce(sample_source, 'FTD_NULL') AS "ftd_sample_type", -- flag nulls for analysis
  coalesce(curie, 'Needs Handling')::text AS "ftd_flag_sample_type", -- flag unhandled strings
  
  lower(sample_source) as "ftd_sample_sources",
  NULL::text as "availablity_status",
  NULL::text as "quantity_number",
  NULL::text as "quantity_units",
  'ap' || '_' || md5('cmg_yale' || '|' || cast(coalesce(consent_id, '') as text))::text as "has_access_policy",
  'sm' || '_' || md5('cmg_yale' || '|' || cast(coalesce(subject_id, '') as text) || '|' || 'cmg_yale' || '|' || cast(coalesce(sample_id, '') as text))::text as "id",
  'sb' || '_' || md5('cmg_yale' || '|' || cast(coalesce(subject_id, '') as text))::text as "subject_id",
--   {  { generate_global_id(prefix='bc',descriptor=['sample_id','{TBD}'], study_id='cmg_yale') }}::text as "biospecimen_collection_id"
  NULL::text as "biospecimen_collection_id"
from (select distinct subject_id, consent_id, sample_source, sample_id, curie 
      from "dbt"."main_main"."cmg_yale_stg_sample"
      left join 
      "dbt"."main"."sm_sample_type"
      on lower(sample_source) = src_format
      ) as sam
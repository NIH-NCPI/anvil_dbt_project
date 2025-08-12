

select 
  distinct
--   { { generate_global_id(prefix='sm',descriptor=['sample_id'], study_id='cmg_bh') }}::text as "parent_sample",
  NULL::text as "parent_sample",
  sample_source::text as "sample_type",
  NULL::text as "availablity_status",
  NULL::text as "quantity_number",
  NULL::text as "quantity_units",
  'ap' || '_' || md5('cmg_bh' || '|' || cast(coalesce(ingest_provenance, '') as text))::text as "has_access_policy",
  'sm' || '_' || md5('cmg_bh' || '|' || cast(coalesce(sample_id, '') as text) || '|' || 'cmg_bh' || '|' || cast(coalesce(subject_id, '') as text))::text as "id",
  'sb' || '_' || md5('cmg_bh' || '|' || cast(coalesce(subject_id, '') as text))::text as "subject_id",
--   {  { generate_global_id(prefix='bc',descriptor=['sample_id','{TBD}'], study_id='cmg_bh') }}::text as "biospecimen_collection_id"
  NULL::text as "biospecimen_collection_id"
from (select distinct sample_id, subject_id, sample_source, ingest_provenance
    from (select distinct subject_id, ingest_provenance, sample_source, sample_id from "dbt"."main_main"."cmg_bh_stg_sample") as sam
    join (select distinct subject_id, ingest_provenance from "dbt"."main_main"."cmg_bh_stg_subject") as sub
    using (subject_id, ingest_provenance)
      )
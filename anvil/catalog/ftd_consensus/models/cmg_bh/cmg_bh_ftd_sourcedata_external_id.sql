{{ config(materialized='table', schema='cmg_bh_data') }}

select 
  {{ generate_global_id(prefix='sd',descriptor=['sample_source'], study_id='cmg_bh') }}::text as "sourcedata_id",
  NULL::text as "external_id"
from (select distinct ingest_provenance, sample_id, sample_source from {{ ref('cmg_bh_stg_sample') }} )
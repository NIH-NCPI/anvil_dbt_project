{{ config(materialized='table', schema='cmg_yale_data') }}

with 
lookup as (
    select 
      distinct 
      subject_id,
      ingest_provenance,
      family_id,
      proband_relationship
    from (select distinct subject_id, ingest_provenance, family_id, proband_relationship from {{ ref('cmg_yale_stg_subject') }}) s
)

select 
  distinct
  {{ generate_global_id(prefix='fm',descriptor=['family_id','subject_id'], study_id='cmg_yale') }}::text as "familymember_id",
  subject_id::text as "external_id"
from lookup
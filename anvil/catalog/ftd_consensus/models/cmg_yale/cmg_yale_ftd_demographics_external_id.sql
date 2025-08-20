{{ config(materialized='table', schema='cmg_yale_data') }}

select 
   {{ generate_global_id(prefix='dm',descriptor=['subject_id'], study_id='cmg_yale') }}::text as "demographics_id",
   subject_id::text as "external_id"
from (select distinct sex, ancestry, subject_id, ingest_provenance from {{ ref('cmg_yale_stg_subject') }}) as s
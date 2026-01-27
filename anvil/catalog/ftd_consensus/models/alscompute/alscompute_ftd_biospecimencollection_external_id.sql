{{ config(materialized='table', schema='alscompute_data') }}

select 
  {{ generate_global_id(prefix='bc',descriptor=['sample_id'], study_id='alscompute') }}::text as "biospecimencollection_id",
  sample_id::text as "external_id"
from (select distinct sample_id from {{ ref('alscompute_stg_sample') }}) as sample

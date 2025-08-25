{{ config(materialized='table', schema='cmg_yale_data') }}

select  
  distinct
  {{ generate_global_id(prefix='sm',descriptor=['sample_id','subject_id'], study_id='cmg_bh') }}::text as "sample_id",
  sample_id::text as "external_id"
from (select distinct subject_id, sample_source, sample_id 
      from {{ ref('cmg_bh_stg_sample') }}
      ) as sam
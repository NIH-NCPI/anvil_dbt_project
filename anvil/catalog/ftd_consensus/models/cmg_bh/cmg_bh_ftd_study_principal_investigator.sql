{{ config(materialized='table', schema='cmg_bh_data') }}

select 
  {{ generate_global_id(prefix='sd',descriptor=['Baylor Hopkins Center for Mendelian Genomics (BH CMG)'], study_id='cmg_bh') }}::text as "study_id",
  principal_investigator::text as "principal_investigator"
from (select distinct principal_investigator {{ ref('cmg_bh_stg_sample') }})
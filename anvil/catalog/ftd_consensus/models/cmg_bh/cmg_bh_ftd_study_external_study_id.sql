{{ config(materialized='table', schema='cmg_bh_data') }}

select 
  {{ generate_global_id(prefix='sd',descriptor=['dbgap_study_id'], study_id='cmg_bh') }}::text as "id"
  dbgap_study_id::text as "external_study_id"
from (select distinct dbgap_study_id from {{ ref('cmg_bh_stg_subject') }}) s
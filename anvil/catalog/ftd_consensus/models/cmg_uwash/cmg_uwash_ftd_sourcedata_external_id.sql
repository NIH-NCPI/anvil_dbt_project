{{ config(materialized='table', schema='cmg_uwash_data') }}

select 
  {{ generate_global_id(prefix='sd',descriptor=['dbgap_study_id'], study_id='phs000693') }}::text as "sourcedata_id",
  dbgap_study_id::text as "external_id"
from (select distinct dbgap_study_id from {{ ref('cmg_uwash_stg_subject') }}) as subject
where dbgap_study_id is not null
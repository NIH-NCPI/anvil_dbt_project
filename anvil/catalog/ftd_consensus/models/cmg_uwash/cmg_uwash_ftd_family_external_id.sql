{{ config(materialized='table', schema='cmg_uwash_data') }}

select 
  {{ generate_global_id(prefix='fy',descriptor=['family_id'], study_id='phs000693') }}::text as "family_id",
  family_id::text as "external_id"
from (select distinct family_id from {{ ref('cmg_uwash_stg_subject') }}) as sub


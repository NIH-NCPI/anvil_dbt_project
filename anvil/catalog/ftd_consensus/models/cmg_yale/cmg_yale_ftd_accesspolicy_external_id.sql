{{ config(materialized='table', schema='cmg_yale_data') }}

select 
  {{ generate_global_id(prefix='ap',descriptor=['consent_id'], study_id='cmg_yale') }}::text as "accesspolicy_id",
  consent_id::text as "external_id"
from (select distinct consent_id from {{ ref('cmg_yale_stg_subject') }}) as s
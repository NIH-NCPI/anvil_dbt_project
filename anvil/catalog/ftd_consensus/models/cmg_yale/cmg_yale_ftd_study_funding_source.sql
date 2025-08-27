{{ config(materialized='table', schema='cmg_yale_data') }}

select 
  {{ generate_global_id(prefix='',descriptor=['dbgap_study_id'], study_id='cmg_yale') }}::text as "study_id",
  NULL::text as "funding_source"
from (select distinct dbgap_study_id from {{ ref('cmg_yale_stg_subject') }} where dbgap_study_id is not null ) as s
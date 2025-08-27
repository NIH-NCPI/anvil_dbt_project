{{ config(materialized='table', schema='cmg_yale_data') }}

select 
  NULL::text as "parent_study_id",
  'Yale Center for Mendelian Genomics (Y CMG)' as "study_title", 
  {{ generate_global_id(prefix='st',descriptor=['dbgap_study_id'], study_id='cmg_yale') }}::text as "id"
from (select distinct dbgap_study_id from {{ ref('cmg_yale_stg_subject') }} where dbgap_study_id is not null ) as s
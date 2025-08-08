{{ config(materialized='table', schema='cmg_bh_data') }}

select 
  {{ generate_global_id(prefix='sd',descriptor=['id'], study_id='cmg_bh') }}::text as "study_id",
  project_investigator::text as "principal_investigator"
from (select distinct 
        project_investigator,
        'Baylor Hopkins Center for Mendelian Genomics (BH CMG)' as "id" 
      from {{ ref('cmg_bh_stg_subject') }}) as s
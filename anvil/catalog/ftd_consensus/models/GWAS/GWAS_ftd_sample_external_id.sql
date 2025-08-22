{{ config(materialized='table', schema='GWAS_data') }}

select 
  {{ generate_global_id(prefix='sm',descriptor=['samplesubjectmapping.subject_id', 'samplesubjectmapping.sample_id'], study_id='phs001584') }}::text as  "sample_id",
  samplesubjectmapping.sample_id::text as "external_id"
from {{ ref('GWAS_stg_samplesubjectmapping') }} as samplesubjectmapping


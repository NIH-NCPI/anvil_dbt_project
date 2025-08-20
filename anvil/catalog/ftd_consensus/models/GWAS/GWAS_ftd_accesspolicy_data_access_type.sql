{{ config(materialized='table', schema='GWAS_data') }}

select 
  {{ generate_global_id(prefix='',descriptor=[''], study_id='GWAS') }}::text as "accesspolicy_id",
  GEN_UNKNOWN.data_access_type::text as "data_access_type"
from {{ ref('GWAS_stg_subjectconsent') }} as subjectconsent


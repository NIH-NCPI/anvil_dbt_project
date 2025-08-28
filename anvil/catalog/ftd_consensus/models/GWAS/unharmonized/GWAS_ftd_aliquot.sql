{{ config(materialized='table', schema='GWAS_data') }}

select 
GEN_UNKNOWN.availablity_status::text as "availablity_status",
  GEN_UNKNOWN.quantity_number::text as "quantity_number",
  GEN_UNKNOWN.quantity_units::text as "quantity_units",
  GEN_UNKNOWN.concentration_number::text as "concentration_number",
  GEN_UNKNOWN.concentration_unit::text as "concentration_unit",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='GWAS') }}::text as "has_access_policy",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='GWAS') }}::text as "id",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='GWAS') }}::text as "sample_id"
from {{ ref('GWAS_stg_bmi') }} as bmi
join {{ ref('GWAS_stg_demographics') }} as demographics
on subjectconsent.subject_id = demographics.subject_id  join {{ ref('GWAS_stg_pedigree') }} as pedigree
on   join {{ ref('GWAS_stg_phecode') }} as phecode
on   join {{ ref('GWAS_stg_sampleattributes') }} as sampleattributes
on samplesubjectmapping.sample_id = sampleattributes.sample_id  join {{ ref('GWAS_stg_samplesubjectmapping') }} as samplesubjectmapping
on sampleattributes.sample_id = samplesubjectmapping.sample_id  join {{ ref('GWAS_stg_subjectconsent') }} as subjectconsent
on demographics.subject_id = subjectconsent.subject_id 


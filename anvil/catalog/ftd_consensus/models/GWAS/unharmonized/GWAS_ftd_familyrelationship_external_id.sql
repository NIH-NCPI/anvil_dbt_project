{{ config(materialized='table', schema='GWAS_data') }}

select 
  {{ generate_global_id(prefix='',descriptor=[''], study_id='GWAS') }}::text as "familyrelationship_id",
  GEN_UNKNOWN.external_id::text as "external_id"
from {{ ref('GWAS_stg_bmi') }} as bmi
join {{ ref('GWAS_stg_demographics') }} as demographics
on subjectconsent.subject_id = demographics.subject_id  join {{ ref('GWAS_stg_pedigree') }} as pedigree
on   join {{ ref('GWAS_stg_phecode') }} as phecode
on   join {{ ref('GWAS_stg_sampleattributes') }} as sampleattributes
on samplesubjectmapping.sample_id = sampleattributes.sample_id  join {{ ref('GWAS_stg_samplesubjectmapping') }} as samplesubjectmapping
on sampleattributes.sample_id = samplesubjectmapping.sample_id  join {{ ref('GWAS_stg_subjectconsent') }} as subjectconsent
on demographics.subject_id = subjectconsent.subject_id 


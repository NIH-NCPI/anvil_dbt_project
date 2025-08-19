{{ config(materialized='table', schema='GWAS_data') }}

select 
GEN_UNKNOWN.disease_limitation::text as "disease_limitation",
  GEN_UNKNOWN.description::text as "description",
  GEN_UNKNOWN.website::text as "website",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='GWAS') }}::text as "id"
from {{ ref('GWAS_stg_bmi') }} as bmi
join {{ ref('GWAS_stg_casecontrol') }} as casecontrol
on   join {{ ref('GWAS_stg_demographics') }} as demographics
on subjectconsent.subject_id = demographics.subject_id  join {{ ref('GWAS_stg_pedigree') }} as pedigree
on   join {{ ref('GWAS_stg_phecode') }} as phecode
on   join {{ ref('GWAS_stg_sampleattributes') }} as sampleattributes
on samplesubjectmapping.sample_id = sampleattributes.sample_id  join {{ ref('GWAS_stg_samplesubjectmapping') }} as samplesubjectmapping
on sampleattributes.sample_id = samplesubjectmapping.sample_id  join {{ ref('GWAS_stg_subjectconsent') }} as subjectconsent
on demographics.subject_id = subjectconsent.subject_id 


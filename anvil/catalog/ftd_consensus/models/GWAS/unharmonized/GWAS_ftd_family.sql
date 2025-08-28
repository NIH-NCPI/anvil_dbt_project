{{ config(materialized='table', schema='GWAS_data') }}

select 
GEN_UNKNOWN.family_type::text as "family_type",
  GEN_UNKNOWN.family_description::text as "family_description",
  GEN_UNKNOWN.consanguinity::text as "consanguinity",
  GEN_UNKNOWN.family_study_focus::text as "family_study_focus",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='GWAS') }}::text as "has_access_policy",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='GWAS') }}::text as "id"
from {{ ref('GWAS_stg_bmi') }} as bmi
join {{ ref('GWAS_stg_demographics') }} as demographics
on subjectconsent.subject_id = demographics.subject_id  join {{ ref('GWAS_stg_pedigree') }} as pedigree
on   join {{ ref('GWAS_stg_phecode') }} as phecode
on   join {{ ref('GWAS_stg_sampleattributes') }} as sampleattributes
on samplesubjectmapping.sample_id = sampleattributes.sample_id  join {{ ref('GWAS_stg_samplesubjectmapping') }} as samplesubjectmapping
on sampleattributes.sample_id = samplesubjectmapping.sample_id  join {{ ref('GWAS_stg_subjectconsent') }} as subjectconsent
on demographics.subject_id = subjectconsent.subject_id 


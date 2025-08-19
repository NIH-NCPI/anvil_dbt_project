{{ config(materialized='table', schema='GWAS_data') }}

select 
GEN_UNKNOWN.date_of_birth::integer as "date_of_birth",
  GEN_UNKNOWN.date_of_birth_type::text as "date_of_birth_type",
  casecontrol.sex::text as "sex",
  GEN_UNKNOWN.sex_display::text as "sex_display",
  GEN_UNKNOWN.race_display::text as "race_display",
  casecontrol.ethnicity::text as "ethnicity",
  GEN_UNKNOWN.ethnicity_display::text as "ethnicity_display",
  GEN_UNKNOWN.age_at_last_vital_status::integer as "age_at_last_vital_status",
  GEN_UNKNOWN.vital_status::text as "vital_status",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='GWAS') }}::text as "has_access_policy",
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


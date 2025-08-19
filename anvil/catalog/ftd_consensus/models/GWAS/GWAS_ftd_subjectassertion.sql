{{ config(materialized='table', schema='GWAS_data') }}

select 
GEN_UNKNOWN.assertion_type::text as "assertion_type",
  GEN_UNKNOWN.age_at_assertion::text as "age_at_assertion",
  GEN_UNKNOWN.age_at_event::text as "age_at_event",
  GEN_UNKNOWN.age_at_resolution::text as "age_at_resolution",
  GEN_UNKNOWN.code::text as "code",
  GEN_UNKNOWN.display::text as "display",
  GEN_UNKNOWN.value_code::text as "value_code",
  GEN_UNKNOWN.value_display::text as "value_display",
  GEN_UNKNOWN.value_number::text as "value_number",
  GEN_UNKNOWN.value_units::text as "value_units",
  GEN_UNKNOWN.value_units_display::text as "value_units_display",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='GWAS') }}::text as "has_access_policy",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='GWAS') }}::text as "id",
    {{ generate_global_id(prefix='',descriptor=[''], study_id='GWAS') }}::text as "subject_id"
from {{ ref('GWAS_stg_bmi') }} as bmi
join {{ ref('GWAS_stg_casecontrol') }} as casecontrol
on   join {{ ref('GWAS_stg_demographics') }} as demographics
on subjectconsent.subject_id = demographics.subject_id  join {{ ref('GWAS_stg_pedigree') }} as pedigree
on   join {{ ref('GWAS_stg_phecode') }} as phecode
on   join {{ ref('GWAS_stg_sampleattributes') }} as sampleattributes
on samplesubjectmapping.sample_id = sampleattributes.sample_id  join {{ ref('GWAS_stg_samplesubjectmapping') }} as samplesubjectmapping
on sampleattributes.sample_id = samplesubjectmapping.sample_id  join {{ ref('GWAS_stg_subjectconsent') }} as subjectconsent
on demographics.subject_id = subjectconsent.subject_id 


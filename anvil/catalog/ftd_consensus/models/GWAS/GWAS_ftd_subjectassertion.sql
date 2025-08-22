{{ config(materialized='table', schema='GWAS_data') }}

with phecode_cte as (
    select 
'ehr_billing_code' as "assertion_type",
  NULL as "age_at_assertion",
  phecode.age_at_observation as "age_at_event",
  NULL as "age_at_resolution",
  phecode.phecode::text as "code",
  NULL as "display",
  NULL as "value_code",
  NULL as "value_display",
  NULL as "value_number",
  NULL as "value_units",
  NULL as "value_units_display",
    {{ generate_global_id(prefix='ap',descriptor=['subjectconsent.consent'], study_id='phs001584') }}::text as "has_access_policy",
    {{ generate_global_id(prefix='sa',descriptor=['phecode.subject_id', 'phecode.phecode'], study_id='phs001584') }}::text as "id",
    {{ generate_global_id(prefix='sb',descriptor=['phecode.subject_id'], study_id='phs001584') }}::text as "subject_id"
from {{ ref('GWAS_stg_phecode') }} as phecode
left join {{ ref('GWAS_stg_subjectconsent') }} as subjectconsent
using(subject_id) 
  from {{ ref('GWAS_stg_phecode') }} as phecode
 ) 
 
 UNION ALL
 
 bmi_cte as (
 select
  'measurement' as "assertion_type",
  NULL as "age_at_assertion",
  phecode.age_at_observation as "age_at_event",
  NULL as "age_at_resolution",
  phecode.phecode::text as "code",
  NULL as "display",
  NULL as "value_code",
  NULL as "value_display",
  NULL as "value_number",
  NULL as "value_units",
  NULL as "value_units_display",
  {{ generate_global_id(prefix='ap',descriptor=['subjectconsent.consent'], study_id='phs001584') }}::text as "has_access_policy",
  {{ generate_global_id(prefix='',descriptor=[''], study_id='phs001584') }}::text as "id",
  {{ generate_global_id(prefix='',descriptor=[''], study_id='phs001584') }}::text as "subject_id"
from {{ ref('GWAS_stg_phecode') }} as phecode
left join {{ ref('GWAS_stg_subjectconsent') }} as subjectconsent
using(subject_id) 
  from {{ ref('GWAS_stg_bmi') }} as bmi
 )
    


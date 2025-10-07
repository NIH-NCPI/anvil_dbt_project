{{ config(materialized='table', schema='GWAS_data') }}

with bmi_cte as (
    select distinct
        subject_id,
        'measurement' as assertion_type,
        NULL as age_at_assertion,
        age_at_event,
        code,
        value_number,
        value_units,
        mapped_code,
        display,
        code_system,
        local_code
    from {{ ref('GWAS_stg_bmi') }}
),


phecode_cte as (
    select distinct
      phecode.subject_id,
      'ehr_billing_code' as "assertion_type",
      phecode.age_at_observation as "age_at_assertion",
      NULL as "age_at_event",
      phecode.phecode::text as "code",
      NULL as "value_number",
      NULL as "value_units"
     from {{ ref('GWAS_stg_phecode') }} as phecode
     ),
  
 union_data as (
    select 
     subject_id,
     assertion_type,
     age_at_assertion,
     age_at_event,
     code as union_code,
     value_number,
     value_units,
     NULL as mapped_code,
     NULL as display,
     NULL as code_system
     from phecode_cte

    union all 
    
    select 
    subject_id,
    assertion_type,
    age_at_assertion,
    age_at_event,
    bmi.local_code as union_code,
    value_number,
    value_units,
    bmi.mapped_code,
    display,
    bmi.code_system
    from bmi_cte as bmi
)

    select distinct 
    assertion_type,
    age_at_assertion,
    age_at_event,
    null as "age_at_resolution",
    CASE 
        WHEN LOWER(cast(ud.code_system AS VARCHAR)) LIKE '%loinc%' THEN CONCAT('LOINC:', ud.mapped_code)
        WHEN ph.phecode IS NOT NULL THEN ph.phecode
        ELSE ud.mapped_code
    END AS code,        
    CASE 
       WHEN ud.display IS NOT NULL THEN ud.display
       ELSE NULL
    END AS display,
    NULL as value_code, 
    NULL as value_display, 
    value_number,
    CASE 
       WHEN ud.value_units IS NOT NULL THEN ud.value_units
       ELSE null
    END AS value_units,  
    CASE 
        WHEN ud.value_units = 'Kilograms' THEN 'kilogram'
        WHEN ud.value_units = 'cm' THEN 'centimeter'
        WHEN ud.value_units = 'kg/m2' THEN 'kilogram per square meter'   
        ELSE NULL
    END AS value_units_display,
    {{ generate_global_id(prefix='sa', descriptor=['ud.subject_id', 'ud.union_code'], study_id='phs001584') }}::text as "id",
    {{ generate_global_id(prefix='ap', descriptor=['subjectconsent.consent'], study_id='phs001584') }}::text as "has_access_policy",
    {{ generate_global_id(prefix='sb', descriptor=['ud.subject_id'], study_id='phs001584') }}::text as "subject_id"
    from union_data as ud
    left join {{ ref('GWAS_stg_subjectconsent') }} as subjectconsent
    on ud.subject_id = subjectconsent.subject_id    
    left join {{ ref('GWAS_stg_phecode') }} as ph
    on ud.union_code = ph.phecode::text

{{ config(materialized='table', schema='GWAS_data') }}

{% set relation_bmi = ref('GWAS_stg_bmi') %}
{% set constant_bmi_columns = ['ftd_index', 'subject_id', 'bmi_observation_age', 'visit_number'] %}
{% set pivot_bmi_columns = get_columns(relation=relation_bmi, exclude=constant_bmi_columns) %}

with phecode_cte as (
    select distinct
      phecode.subject_id,
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
     from {{ ref('GWAS_stg_phecode') }} as phecode
     ),
 
unpivot_bmi as (

        {% for col in pivot_bmi_columns %}
            select distinct
            bmi.subject_id,
            bmi.bmi_observation_age::text as "age_at_assertion",
            NULL as "age_at_event",
            NULL as "age_at_resolution",
            '{{ col }}' AS "code",
            NULL as "display",
            NULL AS "value_code",
            NULL AS "value_display",
           {{ col }}::text as "value_number",
            NULL as "value_units",
            NULL as "value_units_display"
            from {{ ref('GWAS_stg_bmi') }} as bmi
            {% if not loop.last %}union all{% endif %}
        {% endfor %}
    ),
  
 union_data as (
    select * from phecode_cte as pc

    union all 
    
    select * from unpivot_bmi as ub
)

    select distinct 
        CASE WHEN code IN ('weight', 'height', 'body_mass_index') THEN 'measurement' 
             WHEN code IS NOT NULL THEN 'ehr_billing_code'
             ELSE CONCAT('FTD_FLAG: unhandled assertion_type: ',code)
        END::text as "assertion_type",
        age_at_assertion,
        age_at_event, 
        null as "age_at_resolution",
        CASE 
            WHEN code = 'weight'THEN 'LOINC:29463-7'
            WHEN code = 'height' THEN 'LOINC:8302-2'
            WHEN code = 'body_mass_index' THEN 'LOINC:39156-5'
            ELSE code
        END AS code,        
        CASE 
            WHEN code = 'weight' THEN 'Body weight'
            WHEN code = 'height' THEN 'Body height'
            WHEN code = 'body_mass_index' THEN 'Body mass index (BMI) [Ratio]' 
            ELSE display
        END AS display,
        value_code, 
        value_display, 
        value_number,
        CASE 
            WHEN code = 'weight' THEN 'kg'
            WHEN code = 'height' THEN 'cm'
            WHEN code = 'body_mass_index' THEN 'kg/m2'   
            ELSE NULL
        END AS "value_units",  
        CASE 
            WHEN code = 'weight' THEN 'kilogram'
            WHEN code = 'height' THEN 'centimeter'
            WHEN code = 'body_mass_index' THEN 'kilogram per square meter'   
            ELSE NULL
        END as "value_units_display",
        {{ generate_global_id(prefix='sa', descriptor=['subject_id', 'code'], study_id='phs001584') }}::text as "id",
        {{ generate_global_id(prefix='ap', descriptor=['subjectconsent.consent'], study_id='phs001584') }}::text as "has_access_policy",
        {{ generate_global_id(prefix='sb', descriptor=['subject_id'], study_id='phs001584') }}::text as "subject_id"
    from union_data as ud
    left join {{ ref('GWAS_stg_subjectconsent') }} as subjectconsent
         using (subject_id)


{{ config(materialized='table', schema='GWAS_data') }}

{% set relation_bmi = ref('GWAS_stg_bmi') %}
{% set constant_bmi_columns = ['ftd_index', 'subject_id', 'bmi_observation_age', 'visit_number'] %}
{% set pivot_bmi_columns = get_columns(relation=relation_bmi, exclude=constant_bmi_columns) %}

with bmi_cte as (

        {% for col in pivot_bmi_columns %}
            select distinct
            bmi.subject_id,
            'measurement' as "assertion_type",
            NULL::text as "age_at_assertion",
            bmi.bmi_observation_age as "age_at_event",
            '{{ col }}' AS "code",
           {{ col }}::text as "value_number",
            from {{ ref('GWAS_stg_bmi') }} as bmi
            {% if not loop.last %}union all{% endif %}
        {% endfor %}
    ),
    
bmi_w_units as (
    select distinct
        bc.*,
        bu.units as value_units,
        bm."local code",
        bm.code as mapped_code,
        bm.display,
        bm."code system"
    from bmi_cte as bc
    left join {{ ref('gwas_bmi_seed') }} as bu
        on bc.code = bu.variable_name
    left join (
        select 
            "local code",
            code,
            display,
            "code system",
        from {{ ref('gwas_bmi_mappings') }}
    ) as bm
        on bu.variable_name = bm."local code"
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
    bwu."local code" as union_code,
    value_number,
    value_units,
    bwu.mapped_code,
    display,
    bwu."code system" as code_system
    from bmi_w_units as bwu
)

    select distinct 
    assertion_type,
    age_at_assertion,
    age_at_event,
    null as "age_at_resolution",
    CASE 
        WHEN LOWER(cast(ud."code_system" AS VARCHAR)) LIKE '%loinc%' THEN CONCAT('LOINC:', ud.mapped_code)
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
    {{ generate_global_id(prefix='sa', descriptor=['subject_id', 'ud.union_code'], study_id='phs001584') }}::text as "id",
    {{ generate_global_id(prefix='ap', descriptor=['subjectconsent.consent'], study_id='phs001584') }}::text as "has_access_policy",
    {{ generate_global_id(prefix='sb', descriptor=['subject_id'], study_id='phs001584') }}::text as "subject_id"
    from union_data as ud
    left join {{ ref('GWAS_stg_subjectconsent') }} as subjectconsent
    using(subject_id)
    left join {{ ref('GWAS_stg_phecode') }} as ph
    on ud.union_code = ph.phecode::text

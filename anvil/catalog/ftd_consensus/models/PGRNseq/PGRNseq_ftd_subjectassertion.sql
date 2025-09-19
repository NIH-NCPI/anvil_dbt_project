{{ config(materialized='table', schema='PGRNseq_data') }}

{% set bmi_relation = ref('PGRNseq_stg_bmi') %}
{% set bmi_constant_columns = ['ftd_index', 'subject_id', 'age_observation', 'visit_number'] %}
{% set bmi_pivot_columns = get_columns(relation=bmi_relation, exclude=bmi_constant_columns) %}

{% set ecg_relation = ref('PGRNseq_stg_ecg') %}
{% set ecg_constant_columns = ['ftd_index', 'subject_id', 'age_at_ecg', 'visit_number'] %}
{% set ecg_pivot_columns = get_columns(relation=ecg_relation, exclude=ecg_constant_columns) %}


with bmi_cte as (
    {% for col in bmi_pivot_columns %}
        select distinct
        bmi.subject_id,
        NULL::text as "age_at_assertion",
        bmi.age_observation as "age_at_event",
        NULL as "age_at_resolution",
        '{{ col }}' AS "code",
        NULL as "display",
        NULL AS "value_code",
        NULL AS "value_display",
        {{ col }}::text as "value_number",
        NULL as "value_units",
        NULL as "value_units_display"
        from {{ bmi_relation }} as bmi
        {% if not loop.last %}union all{% endif %}
    {% endfor %}
),

ecg_units as (
    select 
        variable_name as code, 
        units as value_units
    from {{ ref('ecg_seed') }}),
    
ecg_cte as (
    {% for col in ecg_pivot_columns %}
        select distinct
        ecg.subject_id,
        NULL::text as "age_at_assertion",
        ecg.age_at_ecg as "age_at_event",
        NULL as "age_at_resolution",
        '{{ col }}' AS "code",
        null as "display",
        NULL AS "value_code",
        NULL AS "value_display",
        {{ col }}::text as "value_number",
        NULL as "value_units_display"
        from {{ ecg_relation }} as ecg
        {% if not loop.last %}union all{% endif %}
    {% endfor %}
),

ecg_w_units as (
    select distinct
        ec.*,
        ecg_units.value_units as 'value_units'
    from ecg_cte as ec
    left join ecg_units
        on ec.code = ecg_units.code
),

union_data as (
    select * from bmi_cte as bc
    union all 
    select * from ecg_w_units as ecu
)

select distinct
    'measurement' as "assertion_type",
    age_at_assertion,
    age_at_event, 
    null as "age_at_resolution",
    CASE 
        WHEN ud.code = 'weight' THEN 'LOINC:29463-7'
        WHEN ud.code = 'height' THEN 'LOINC:8302-2'
        WHEN ud.code = 'body_mass_index' THEN 'LOINC:39156-5'
        WHEN LOWER(CAST(ep."code system" AS VARCHAR)) LIKE '%loinc%' THEN CONCAT('LOINC:', ep.code)
        ELSE ep.code
    END AS code,        
    CASE 
        WHEN ud.code = 'weight' THEN 'Body weight'
        WHEN ud.code = 'height' THEN 'Body height'
        WHEN ud.code = 'body_mass_index' THEN 'Body mass index (BMI) [Ratio]' 
        ELSE ep.display
    END AS display,
    value_code, 
    value_display, 
    value_number,
    CASE 
        WHEN ud.code = 'weight' THEN 'kg'
        WHEN ud.code = 'height' THEN 'cm'
        WHEN ud.code = 'body_mass_index' THEN 'kg/m2'
        ELSE ud.value_units
    END AS "value_units",  
    CASE 
        WHEN ud.code = 'weight' THEN 'kilogram'
        WHEN ud.code = 'height' THEN 'centimeter'
        WHEN ud.code = 'body_mass_index' THEN 'kilogram per square meter'   
        ELSE NULL
    END as "value_units_display",
    {{ generate_global_id(prefix='sa', descriptor=['subject_id', 'ud.code'], study_id='phs000906') }}::text as "id",
    {{ generate_global_id(prefix='ap', descriptor=['subjectconsent.consent'], study_id='phs000906') }}::text as "has_access_policy",
    {{ generate_global_id(prefix='sb', descriptor=['subject_id'], study_id='phs000906') }}::text as "subject_id"
from union_data as ud
left join {{ ref('PGRNseq_stg_subjectconsent') }} as subjectconsent
     using (subject_id)
left join {{ ref('ecg_pgrnseq') }} as ep
on ud.code = ep.local_code



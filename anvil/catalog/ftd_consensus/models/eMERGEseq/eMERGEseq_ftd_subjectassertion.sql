{{ config(materialized='table', schema='eMERGEseq_data') }}

{% set relation = ref('eMERGEseq_stg_phecode') %}
{% set constant_columns = ['subject_id', 'ftd_index'] %}
{% set condition_columns = get_columns(relation=relation, exclude=constant_columns) %} 

{% set relation_bmi = ref('eMERGEseq_stg_bmi') %}
{% set constant_bmi_columns = ['ftd_index', 'subject_id', 'age_at_observation', 'visit_number'] %}
{% set pivot_bmi_columns = get_columns(relation=relation_bmi, exclude=constant_bmi_columns) %}
with lookup as(
    select 
        LOWER(variable_name) as code, 
        description as display
    from {{ ref('phecode_seed') }}),

     
unpivot_df as (

    {% for col in condition_columns %}
        select
            {{ constant_columns | join(', ') }},
            '{{ col }}' as code,
            case when "{{ col }}" = 0 then 'LA9634-2'
                 when "{{ col }}" = 1 then 'LA9633-4'
            end as value_code,
            case when "{{ col }}" = 0 then 'Absent'
                 when "{{ col }}" = 1 then 'Present'
            end as value_display,
        from {{ ref('eMERGEseq_stg_phecode') }} as p
        {% if not loop.last %}union all{% endif %}
    {% endfor %}
),

unpivoted_with_display as (
    select distinct
        up.*,
        lookup.display as 'display'
    from unpivot_df as up
    left join lookup
        on up.code = lookup.code
),

unpivot_bmi as (

        {% for col in pivot_bmi_columns %}
            select distinct
            {{ constant_bmi_columns | join(', ') }},
            'clinical_finding'::text as "assertion_type",
            bmi.age_at_observation::text as "age_at_assertion",
            null as "age_at_event", 
            null as "age_at_resolution",
            CASE 
                WHEN '{{ col }}' = 'weight' THEN 'LOINC:29463-7'
                WHEN '{{ col }}' = 'height' THEN 'LOINC:8302-2'
                WHEN '{{ col }}' = 'body_mass_index' THEN 'LOINC:39156-5'                
            END AS code,
            CASE 
                WHEN '{{ col }}' = 'weight' THEN 'Body weight'
                WHEN '{{ col }}' = 'height' THEN 'Body height'
                WHEN '{{ col }}' = 'body_mass_index' THEN 'Body mass index (BMI) [Ratio]'                
            END AS display,
            CASE 
                WHEN "{{ col }}" IS NULL OR "{{ col }}" = '.' THEN 'LA9634-2'
                ELSE 'LA9633-4'
            END AS value_code,
            CASE
                WHEN "{{ col }}" IS NULL OR "{{ col }}" = '.' THEN 'Absent'
                ELSE 'Present'
            END AS value_display,
           "{{ col }}"::text as "value_number",
            CASE 
                WHEN '{{ col }}' = 'weight' THEN 'kg'
                WHEN '{{ col }}' = 'height' THEN 'cm'
                WHEN '{{ col }}' = 'body_mass_index' THEN 'kg/m2'                
            END AS "value_units",
            CASE 
                WHEN '{{ col }}' = 'weight' THEN 'kilogram'
                WHEN '{{ col }}' = 'height' THEN 'centimeter'
                WHEN '{{ col }}' = 'body_mass_index' THEN 'kilogram per square meter'                
            END AS  "value_units_display",
            from {{ ref('eMERGEseq_stg_bmi') }} as bmi
            {% if not loop.last %}union all{% endif %}
        {% endfor %}
    ),

source as (
    select distinct 
        'ehr_billing_code'::text as "assertion_type",
        null as "age_at_assertion",
        null as "age_at_event", 
        null as "age_at_resolution",
        uwd.code, 
        uwd.display, 
        uwd.value_code, 
        uwd.value_display, 
        null as "value_number",
        null as "value_units",
        null as "value_units_display",
        {{ generate_global_id(prefix='sa', descriptor=['uwd.subject_id', 'uwd.code'], study_id='phs001616') }}::text as "id",
        {{ generate_global_id(prefix='ap', descriptor=['subjectconsent.consent'], study_id='phs001616') }}::text as "has_access_policy",
        {{ generate_global_id(prefix='sb', descriptor=['uwd.subject_id'], study_id='phs001616') }}::text as "subject_id"
    from unpivoted_with_display as uwd
    left join {{ ref('eMERGEseq_stg_subjectconsent') }} as subjectconsent
        on uwd.subject_id = subjectconsent.subject_id
    
    union all
    select distinct
        ub.assertion_type, 
        ub.age_at_assertion, 
        ub.age_at_event, 
        ub.age_at_resolution, 
        ub.code,
        ub.display, 
        ub.value_code, 
        ub.value_display, 
        ub.value_number, 
        ub.value_units, 
        ub.value_units_display, 
        {{ generate_global_id(prefix='sa', descriptor=['bmi.subject_id', 'ub.code'], study_id='phs001616') }}::text as "id",
        {{ generate_global_id(prefix='ap', descriptor=['subjectconsent.consent'], study_id='phs001616') }}::text as "has_access_policy",
        {{ generate_global_id(prefix='sb', descriptor=['bmi.subject_id'], study_id='phs001616') }}::text as "subject_id"
    from unpivot_bmi as ub
    left join {{ relation_bmi }} as bmi 
        on ub.subject_id = bmi.subject_id
    left join {{ ref('eMERGEseq_stg_subjectconsent') }} as subjectconsent
        on bmi.subject_id = subjectconsent.subject_id
        
)

select *
from source
{{ config(materialized='table', schema='PGRNseq_data') }}

{% set bmi_relation = ref('PGRNseq_stg_bmi') %}
{% set bmi_constant_columns = ['ftd_index', 'subject_id', 'age_observation', 'visit_number'] %}
{% set bmi_pivot_columns = get_columns(relation=bmi_relation, exclude=bmi_constant_columns) %}

{% set ecg_relation = ref('PGRNseq_stg_ecg') %}
{% set ecg_constant_columns = ['ftd_index', 'subject_id', 'age_at_ecg', 'visit_number'] %}
{% set ecg_pivot_columns = get_columns(relation=ecg_relation, exclude=ecg_constant_columns) %}

{% set lab_relation = ref('PGRNseq_stg_labs') %}
{% set lab_constant_columns = ['ftd_index', 'subject_id', 'age_at_lipids', 'visit_number'] %}
{% set lab_pivot_columns = get_columns(relation=lab_relation, exclude=lab_constant_columns) %}

{% set bmi_mappings = ref('bmi_mappings') %}
{% set ecg_mappings = ref('ecg_mappings') %}
{% set labs_mappings = ref('labs_mappings') %}

{% set bmi_units = ref('bmi_seed') %}
{% set ecg_units = ref('ecg_seed') %}
{% set lab_units = ref('labs_seed') %}


with bmi_cte as (
    {% for col in bmi_pivot_columns %}
        select distinct
        bmi.subject_id,
        NULL::text as age_at_assertion,
        bmi.age_observation as age_at_event,
        NULL as age_at_resolution,
        '{{ col }}' as code,
        NULL as display,
        NULL as value_code,
        NULL as value_display,
        {{ col }}::text as value_number,
        NULL as value_units_display,
        NULL as value_units
        from {{ bmi_relation }} as bmi
        {% if not loop.last %}union all{% endif %}
    {% endfor %}
),

bmi_w_units as (
    select distinct
        bc.*,
        bu.units as value_units
    from bmi_cte as bc
    left join {{ bmi_units }} as bu
        on bc.code = bu.variable_name
),

ecg_cte as (
    {% for col in ecg_pivot_columns %}
        select distinct
        ecg.subject_id,
        NULL::text as age_at_assertion,
        ecg.age_at_ecg as age_at_event,
        NULL as age_at_resolution,
        '{{ col }}' as code,
        NULL as display,
        NULL as value_code,
        NULL as value_display,
        {{ col }}::text as value_number,
        NULL as value_units_display
        from {{ ecg_relation }} as ecg
        {% if not loop.last %}union all{% endif %}
    {% endfor %}
),

ecg_w_units as (
    select distinct
        ec.*,
        eu.units as value_units
    from ecg_cte as ec
    left join {{ ecg_units }} as eu
        on ec.code = eu.variable_name
),

lab_cte as (
    {% for col in lab_pivot_columns %}
        select distinct
        lab.subject_id,
        NULL::text as age_at_assertion,
        lab.age_at_lipids as age_at_event,
        NULL as age_at_resolution,
        '{{ col }}' as code,
        NULL as display,
        NULL as value_code,
        NULL as value_display,
        {{ col }}::text as value_number,
        NULL as value_units_display
        from {{ lab_relation }} as lab
        {% if not loop.last %}union all{% endif %}
    {% endfor %}
),

lab_w_units as (
    select distinct
        lc.*,
        lu.units as value_units
    from lab_cte as lc
    left join {{ lab_units }} as lu
        on lc.code = lu.variable_name
),

-- Normalized mapping tables (ensuring same structure)
bmi_mapped as (
    select 
        bm."local code" as code,
        bm.display,
        bm."code system",
        bu.units as value_units
    from {{ bmi_mappings }} as bm
    left join {{ bmi_units }} as bu
        on bm."local code" = bu.variable_name
),

ecg_mapped as (
    select 
        em."local code" as code,
        em.display,
        em."code system",
        eu.units as value_units
    from {{ ecg_mappings }} as em
    left join {{ ecg_units }} as eu
        on em."local code" = eu.variable_name
),

lab_mapped as (
    select 
        lm."local code" as code,
        lm.display,
        lm."code system",
        lu.units as value_units
    from {{ labs_mappings }} as lm
    left join {{ lab_units }} as lu
        on lm."local code" = lu.variable_name
),

union_data as (
    select * from bmi_w_units
    union all
    select * from ecg_w_units
    union all
    select * from lab_w_units
)

select distinct
    'measurement' as assertion_type,
    age_at_assertion,
    age_at_event, 
    null as age_at_resolution,

    case 
        when lower(cast(mappings."code system" as varchar)) like '%loinc%' then concat('LOINC:', mappings.code)
        else mappings.code
    end as code,        

    mappings.display as display,
    value_code, 
    value_display, 
    value_number,

    mappings.value_units as value_units,  

    case 
        when ud.code = 'weight' then 'kilogram'
        when ud.code = 'height' then 'centimeter'
        when ud.code = 'body_mass_index' then 'kilogram per square meter'   
        when ud.value_units = 'ms' then 'millisecond'
        when ud.value_units = 'bpm' then 'heart beats per minute'
        else null
    end as value_units_display,

    {{ generate_global_id(prefix='sa', descriptor=['subject_id', 'ud.code'], study_id='phs000906') }}::text as id,
    {{ generate_global_id(prefix='ap', descriptor=['subjectconsent.consent'], study_id='phs000906') }}::text as has_access_policy,
    {{ generate_global_id(prefix='sb', descriptor=['subject_id'], study_id='phs000906') }}::text as subject_id

from union_data as ud
left join {{ ref('PGRNseq_stg_subjectconsent') }} as subjectconsent
    using (subject_id)
left join (
    select code, display, "code system", value_units from bmi_mapped
    union all
    select code, display, "code system", value_units from ecg_mapped
    union all
    select code, display, "code system", value_units from lab_mapped
) as mappings
    using (code)















with bmi_cte as (
    
),

bmi_w_units as (
    select distinct
        bc.*,
        bu.units as value_units
    from bmi_cte as bc
    left join "dbt"."main"."bmi_seed" as bu
        on bc.code = bu.variable_name
),

ecg_cte as (
    
),

ecg_w_units as (
    select distinct
        ec.*,
        eu.units as value_units
    from ecg_cte as ec
    left join "dbt"."main"."ecg_seed" as eu
        on ec.code = eu.variable_name
),

lab_cte as (
    
),

lab_w_units as (
    select distinct
        lc.*,
        lu.units as value_units
    from lab_cte as lc
    left join "dbt"."main"."labs_seed" as lu
        on lc.code = lu.variable_name
),

union_data as (
    select * from bmi_w_units
    union all
    select * from ecg_w_units
    union all
    select * from lab_w_units
)

select distinct
    'measurement' AS assertion_type,
    NULL as age_at_assertion,
    age_at_event, 
    NULL as age_at_resolution,
    CASE 
        WHEN LOWER(cast(mappings."code system" AS VARCHAR)) LIKE '%loinc%' THEN CONCAT('LOINC:', mappings.code)
        ELSE mappings.code
    END as code,        
    mappings.display AS display,
    CASE 
        WHEN mappings.code = 'SNOMED:439631004' THEN
            CASE ud.value_number
                WHEN '0' THEN 'SNOMED:715036001'
                WHEN '1' THEN 'SNOMED:129019007'
                ELSE NULL
            END
        ELSE NULL
    END AS value_code,
    CASE
        WHEN mappings.code = 'SNOMED:439631004' THEN
            CASE ud.value_number
                WHEN 0 THEN 'Does not take medication'
                WHEN 1 THEN 'Taking medication'
                ELSE NULL
            END
        ELSE NULL
    END AS value_display, 
    value_number,
    mappings.value_units AS value_units,  
    CASE 
        WHEN ud.value_units = 'Kilograms' THEN 'kilogram'
        WHEN ud.value_units = 'cm' THEN 'centimeter'
        WHEN ud.value_units = 'kg/m2' THEN 'kilogram per square meter'   
        WHEN ud.value_units = 'ms' THEN 'millisecond'
        WHEN ud.value_units = 'bpm' THEN 'heart beats per minute'
        WHEN ud.value_units = 'mg/dL' THEN 'milligram per deciliter'
        ELSE NULL
    END AS value_units_display,
    'sa' || '_' || md5('phs000906' || '|' || cast(coalesce(subject_id, '') as text) || '|' || 'phs000906' || '|' || cast(coalesce(mappings.code, '') as text) || '|' || 'phs000906' || '|' || cast(coalesce(subjectconsent.consent, '') as text))::text as id,
    'ap' || '_' || md5('phs000906' || '|' || cast(coalesce(subjectconsent.consent, '') as text))::text as has_access_policy,
    'sb' || '_' || md5('phs000906' || '|' || cast(coalesce(subject_id, '') as text) || '|' || 'phs000906' || '|' || cast(coalesce(subjectconsent.consent, '') as text))::text as subject_id
from union_data as ud
left join "dbt"."main_main"."PGRNseq_stg_subjectconsent" as subjectconsent
    using (subject_id)
left join (
    select 
        "local code" as local_code,
        code,
        display,
        "code system",
        value_units
    from "dbt"."main"."bmi_mappings"

    union all

    select 
        "local code" as local_code,
        code,
        display,
        "code system",
        value_units
    from "dbt"."main"."labs_mappings"

    union all

    select 
        local_code,
        code,
        display,
        "code system",
        value_units
    from "dbt"."main"."ecg_mappings"
) as mappings
    on ud.code = mappings.local_code
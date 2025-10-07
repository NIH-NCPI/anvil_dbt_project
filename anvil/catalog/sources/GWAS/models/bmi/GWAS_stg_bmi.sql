{{ config(materialized='table') }}

with bmi_cte as (
    select distinct
    subject_id,
    'measurement' as "assertion_type",
    NULL::text as "age_at_assertion",
    bmi_observation_age as "age_at_event",
    'weight' AS "code",
   weight::text as "value_number",
    from {{ source('GWAS','GWAS_BMI_DS_20201028') }}
    union all

    select distinct
    subject_id,
    'measurement' as "assertion_type",
    NULL::text as "age_at_assertion",
    bmi_observation_age as "age_at_event",
    'height' AS "code",
   height::text as "value_number",
    from {{ source('GWAS','GWAS_BMI_DS_20201028') }}
    union all

    select distinct
    subject_id,
    'measurement' as "assertion_type",
    NULL::text as "age_at_assertion",
    bmi_observation_age as "age_at_event",
    'body_mass_index' AS "code",
   body_mass_index::text as "value_number",
    from {{ source('GWAS','GWAS_BMI_DS_20201028') }} 
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
)

select 
  ROW_NUMBER() OVER () AS ftd_index,
    subject_id,
    age_at_event,
    code,
    value_number,
    value_units,
    mapped_code,
    display,
    "local code" as local_code,
    "code system" as code_system,
from bmi_w_units
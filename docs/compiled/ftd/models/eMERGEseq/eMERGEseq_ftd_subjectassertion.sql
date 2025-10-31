



 




with lookup as(
    select 
        LOWER(variable_name) as code, 
        description as display
    from "dbt"."main"."phecode_seed"),

     
unpivot_df as (

    
        select
            subject_id, ftd_index,
            NULL as "age_at_event",
            'phe_401.1' as code,
            case when "phe_401.1" = 0 then 'LA9634-2'
                 when "phe_401.1" = 1 then 'LA9633-4'
            end as value_code,
            case when "phe_401.1" = 0 then 'Absent'
                 when "phe_401.1" = 1 then 'Present'
            end as value_display,
            NULL as "value_number",
        from "dbt"."main_main"."eMERGEseq_stg_phecode" as p
        union all
    
        select
            subject_id, ftd_index,
            NULL as "age_at_event",
            'phe_250.2' as code,
            case when "phe_250.2" = 0 then 'LA9634-2'
                 when "phe_250.2" = 1 then 'LA9633-4'
            end as value_code,
            case when "phe_250.2" = 0 then 'Absent'
                 when "phe_250.2" = 1 then 'Present'
            end as value_display,
            NULL as "value_number",
        from "dbt"."main_main"."eMERGEseq_stg_phecode" as p
        union all
    
        select
            subject_id, ftd_index,
            NULL as "age_at_event",
            'phe_272.1' as code,
            case when "phe_272.1" = 0 then 'LA9634-2'
                 when "phe_272.1" = 1 then 'LA9633-4'
            end as value_code,
            case when "phe_272.1" = 0 then 'Absent'
                 when "phe_272.1" = 1 then 'Present'
            end as value_display,
            NULL as "value_number",
        from "dbt"."main_main"."eMERGEseq_stg_phecode" as p
        union all
    
        select
            subject_id, ftd_index,
            NULL as "age_at_event",
            'phe_411.4' as code,
            case when "phe_411.4" = 0 then 'LA9634-2'
                 when "phe_411.4" = 1 then 'LA9633-4'
            end as value_code,
            case when "phe_411.4" = 0 then 'Absent'
                 when "phe_411.4" = 1 then 'Present'
            end as value_display,
            NULL as "value_number",
        from "dbt"."main_main"."eMERGEseq_stg_phecode" as p
        union all
    
        select
            subject_id, ftd_index,
            NULL as "age_at_event",
            'phe_495' as code,
            case when "phe_495" = 0 then 'LA9634-2'
                 when "phe_495" = 1 then 'LA9633-4'
            end as value_code,
            case when "phe_495" = 0 then 'Absent'
                 when "phe_495" = 1 then 'Present'
            end as value_display,
            NULL as "value_number",
        from "dbt"."main_main"."eMERGEseq_stg_phecode" as p
        union all
    
        select
            subject_id, ftd_index,
            NULL as "age_at_event",
            'phe_427.21' as code,
            case when "phe_427.21" = 0 then 'LA9634-2'
                 when "phe_427.21" = 1 then 'LA9633-4'
            end as value_code,
            case when "phe_427.21" = 0 then 'Absent'
                 when "phe_427.21" = 1 then 'Present'
            end as value_display,
            NULL as "value_number",
        from "dbt"."main_main"."eMERGEseq_stg_phecode" as p
        union all
    
        select
            subject_id, ftd_index,
            NULL as "age_at_event",
            'phe_530.11' as code,
            case when "phe_530.11" = 0 then 'LA9634-2'
                 when "phe_530.11" = 1 then 'LA9633-4'
            end as value_code,
            case when "phe_530.11" = 0 then 'Absent'
                 when "phe_530.11" = 1 then 'Present'
            end as value_display,
            NULL as "value_number",
        from "dbt"."main_main"."eMERGEseq_stg_phecode" as p
        union all
    
        select
            subject_id, ftd_index,
            NULL as "age_at_event",
            'phe_296.22' as code,
            case when "phe_296.22" = 0 then 'LA9634-2'
                 when "phe_296.22" = 1 then 'LA9633-4'
            end as value_code,
            case when "phe_296.22" = 0 then 'Absent'
                 when "phe_296.22" = 1 then 'Present'
            end as value_display,
            NULL as "value_number",
        from "dbt"."main_main"."eMERGEseq_stg_phecode" as p
        union all
    
        select
            subject_id, ftd_index,
            NULL as "age_at_event",
            'phe_313.1' as code,
            case when "phe_313.1" = 0 then 'LA9634-2'
                 when "phe_313.1" = 1 then 'LA9633-4'
            end as value_code,
            case when "phe_313.1" = 0 then 'Absent'
                 when "phe_313.1" = 1 then 'Present'
            end as value_display,
            NULL as "value_number",
        from "dbt"."main_main"."eMERGEseq_stg_phecode" as p
        union all
    
        select
            subject_id, ftd_index,
            NULL as "age_at_event",
            'phe_244.4' as code,
            case when "phe_244.4" = 0 then 'LA9634-2'
                 when "phe_244.4" = 1 then 'LA9633-4'
            end as value_code,
            case when "phe_244.4" = 0 then 'Absent'
                 when "phe_244.4" = 1 then 'Present'
            end as value_display,
            NULL as "value_number",
        from "dbt"."main_main"."eMERGEseq_stg_phecode" as p
        union all
    
        select
            subject_id, ftd_index,
            NULL as "age_at_event",
            'phe_476' as code,
            case when "phe_476" = 0 then 'LA9634-2'
                 when "phe_476" = 1 then 'LA9633-4'
            end as value_code,
            case when "phe_476" = 0 then 'Absent'
                 when "phe_476" = 1 then 'Present'
            end as value_display,
            NULL as "value_number",
        from "dbt"."main_main"."eMERGEseq_stg_phecode" as p
        union all
    
        select
            subject_id, ftd_index,
            NULL as "age_at_event",
            'phe_272.11' as code,
            case when "phe_272.11" = 0 then 'LA9634-2'
                 when "phe_272.11" = 1 then 'LA9633-4'
            end as value_code,
            case when "phe_272.11" = 0 then 'Absent'
                 when "phe_272.11" = 1 then 'Present'
            end as value_display,
            NULL as "value_number",
        from "dbt"."main_main"."eMERGEseq_stg_phecode" as p
        union all
    
        select
            subject_id, ftd_index,
            NULL as "age_at_event",
            'phe_278.1' as code,
            case when "phe_278.1" = 0 then 'LA9634-2'
                 when "phe_278.1" = 1 then 'LA9633-4'
            end as value_code,
            case when "phe_278.1" = 0 then 'Absent'
                 when "phe_278.1" = 1 then 'Present'
            end as value_display,
            NULL as "value_number",
        from "dbt"."main_main"."eMERGEseq_stg_phecode" as p
        union all
    
        select
            subject_id, ftd_index,
            NULL as "age_at_event",
            'phe_174.11' as code,
            case when "phe_174.11" = 0 then 'LA9634-2'
                 when "phe_174.11" = 1 then 'LA9633-4'
            end as value_code,
            case when "phe_174.11" = 0 then 'Absent'
                 when "phe_174.11" = 1 then 'Present'
            end as value_display,
            NULL as "value_number",
        from "dbt"."main_main"."eMERGEseq_stg_phecode" as p
        union all
    
        select
            subject_id, ftd_index,
            NULL as "age_at_event",
            'phe_296.2' as code,
            case when "phe_296.2" = 0 then 'LA9634-2'
                 when "phe_296.2" = 1 then 'LA9633-4'
            end as value_code,
            case when "phe_296.2" = 0 then 'Absent'
                 when "phe_296.2" = 1 then 'Present'
            end as value_display,
            NULL as "value_number",
        from "dbt"."main_main"."eMERGEseq_stg_phecode" as p
        union all
    
        select
            subject_id, ftd_index,
            NULL as "age_at_event",
            'phe_306' as code,
            case when "phe_306" = 0 then 'LA9634-2'
                 when "phe_306" = 1 then 'LA9633-4'
            end as value_code,
            case when "phe_306" = 0 then 'Absent'
                 when "phe_306" = 1 then 'Present'
            end as value_display,
            NULL as "value_number",
        from "dbt"."main_main"."eMERGEseq_stg_phecode" as p
        union all
    
        select
            subject_id, ftd_index,
            NULL as "age_at_event",
            'phe_327.32' as code,
            case when "phe_327.32" = 0 then 'LA9634-2'
                 when "phe_327.32" = 1 then 'LA9633-4'
            end as value_code,
            case when "phe_327.32" = 0 then 'Absent'
                 when "phe_327.32" = 1 then 'Present'
            end as value_display,
            NULL as "value_number",
        from "dbt"."main_main"."eMERGEseq_stg_phecode" as p
        union all
    
        select
            subject_id, ftd_index,
            NULL as "age_at_event",
            'phe_313.3' as code,
            case when "phe_313.3" = 0 then 'LA9634-2'
                 when "phe_313.3" = 1 then 'LA9633-4'
            end as value_code,
            case when "phe_313.3" = 0 then 'Absent'
                 when "phe_313.3" = 1 then 'Present'
            end as value_display,
            NULL as "value_number",
        from "dbt"."main_main"."eMERGEseq_stg_phecode" as p
        union all
    
        select
            subject_id, ftd_index,
            NULL as "age_at_event",
            'phe_278.11' as code,
            case when "phe_278.11" = 0 then 'LA9634-2'
                 when "phe_278.11" = 1 then 'LA9633-4'
            end as value_code,
            case when "phe_278.11" = 0 then 'Absent'
                 when "phe_278.11" = 1 then 'Present'
            end as value_display,
            NULL as "value_number",
        from "dbt"."main_main"."eMERGEseq_stg_phecode" as p
        union all
    
        select
            subject_id, ftd_index,
            NULL as "age_at_event",
            'phe_272.13' as code,
            case when "phe_272.13" = 0 then 'LA9634-2'
                 when "phe_272.13" = 1 then 'LA9633-4'
            end as value_code,
            case when "phe_272.13" = 0 then 'Absent'
                 when "phe_272.13" = 1 then 'Present'
            end as value_display,
            NULL as "value_number",
        from "dbt"."main_main"."eMERGEseq_stg_phecode" as p
        union all
    
        select
            subject_id, ftd_index,
            NULL as "age_at_event",
            'phe_587' as code,
            case when "phe_587" = 0 then 'LA9634-2'
                 when "phe_587" = 1 then 'LA9633-4'
            end as value_code,
            case when "phe_587" = 0 then 'Absent'
                 when "phe_587" = 1 then 'Present'
            end as value_display,
            NULL as "value_number",
        from "dbt"."main_main"."eMERGEseq_stg_phecode" as p
        
    
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

        
            select distinct
            subject_id, ftd_index,
            bmi.age_at_observation::text as "age_at_event",
            'weight' AS "code",
            NULL AS "value_code",
            NULL AS "value_display",
           weight::text as "value_number",
            NULL as "display"
            from "dbt"."main_main"."eMERGEseq_stg_bmi" as bmi
            union all
        
            select distinct
            subject_id, ftd_index,
            bmi.age_at_observation::text as "age_at_event",
            'height' AS "code",
            NULL AS "value_code",
            NULL AS "value_display",
           height::text as "value_number",
            NULL as "display"
            from "dbt"."main_main"."eMERGEseq_stg_bmi" as bmi
            union all
        
            select distinct
            subject_id, ftd_index,
            bmi.age_at_observation::text as "age_at_event",
            'body_mass_index' AS "code",
            NULL AS "value_code",
            NULL AS "value_display",
           body_mass_index::text as "value_number",
            NULL as "display"
            from "dbt"."main_main"."eMERGEseq_stg_bmi" as bmi
            
        
    ),
    
union_data as (
    select * from unpivoted_with_display as uwd

    union all 
    
    select * from unpivot_bmi as ub
)

    select distinct 
        CASE WHEN code IN ('weight', 'height', 'body_mass_index') THEN 'measurement' 
             WHEN UPPER(code) LIKE 'PHE%' THEN 'ehr_billing_code'
             ELSE CONCAT('FTD_FLAG: unhandled assertion_type: ',code)
        END::text as "assertion_type",
        null as "age_at_assertion",
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
        'sa' || '_' || md5('phs001616' || '|' || cast(coalesce(subject_id, '') as text) || '|' || 'phs001616' || '|' || cast(coalesce(code, '') as text))::text as "id",
        'ap' || '_' || md5('phs001616' || '|' || cast(coalesce(subjectconsent.consent, '') as text))::text as "has_access_policy",
        'sb' || '_' || md5('phs001616' || '|' || cast(coalesce(subject_id, '') as text))::text as "subject_id"
    from union_data as ud
    left join "dbt"."main_main"."eMERGEseq_stg_subjectconsent" as subjectconsent
         using (subject_id)
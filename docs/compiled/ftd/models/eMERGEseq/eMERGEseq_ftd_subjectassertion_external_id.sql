



  





with lookup as(
    select 
        LOWER(variable_name) as code, 
    from "dbt"."main"."phecode_seed"),

     
unpivot_df as (

    
        select distinct
            subject_id, ftd_index,
            'phe_401.1' as code,
        from "dbt"."main_main"."eMERGEseq_stg_phecode" as p
        union all
    
        select distinct
            subject_id, ftd_index,
            'phe_250.2' as code,
        from "dbt"."main_main"."eMERGEseq_stg_phecode" as p
        union all
    
        select distinct
            subject_id, ftd_index,
            'phe_272.1' as code,
        from "dbt"."main_main"."eMERGEseq_stg_phecode" as p
        union all
    
        select distinct
            subject_id, ftd_index,
            'phe_411.4' as code,
        from "dbt"."main_main"."eMERGEseq_stg_phecode" as p
        union all
    
        select distinct
            subject_id, ftd_index,
            'phe_495' as code,
        from "dbt"."main_main"."eMERGEseq_stg_phecode" as p
        union all
    
        select distinct
            subject_id, ftd_index,
            'phe_427.21' as code,
        from "dbt"."main_main"."eMERGEseq_stg_phecode" as p
        union all
    
        select distinct
            subject_id, ftd_index,
            'phe_530.11' as code,
        from "dbt"."main_main"."eMERGEseq_stg_phecode" as p
        union all
    
        select distinct
            subject_id, ftd_index,
            'phe_296.22' as code,
        from "dbt"."main_main"."eMERGEseq_stg_phecode" as p
        union all
    
        select distinct
            subject_id, ftd_index,
            'phe_313.1' as code,
        from "dbt"."main_main"."eMERGEseq_stg_phecode" as p
        union all
    
        select distinct
            subject_id, ftd_index,
            'phe_244.4' as code,
        from "dbt"."main_main"."eMERGEseq_stg_phecode" as p
        union all
    
        select distinct
            subject_id, ftd_index,
            'phe_476' as code,
        from "dbt"."main_main"."eMERGEseq_stg_phecode" as p
        union all
    
        select distinct
            subject_id, ftd_index,
            'phe_272.11' as code,
        from "dbt"."main_main"."eMERGEseq_stg_phecode" as p
        union all
    
        select distinct
            subject_id, ftd_index,
            'phe_278.1' as code,
        from "dbt"."main_main"."eMERGEseq_stg_phecode" as p
        union all
    
        select distinct
            subject_id, ftd_index,
            'phe_174.11' as code,
        from "dbt"."main_main"."eMERGEseq_stg_phecode" as p
        union all
    
        select distinct
            subject_id, ftd_index,
            'phe_296.2' as code,
        from "dbt"."main_main"."eMERGEseq_stg_phecode" as p
        union all
    
        select distinct
            subject_id, ftd_index,
            'phe_306' as code,
        from "dbt"."main_main"."eMERGEseq_stg_phecode" as p
        union all
    
        select distinct
            subject_id, ftd_index,
            'phe_327.32' as code,
        from "dbt"."main_main"."eMERGEseq_stg_phecode" as p
        union all
    
        select distinct
            subject_id, ftd_index,
            'phe_313.3' as code,
        from "dbt"."main_main"."eMERGEseq_stg_phecode" as p
        union all
    
        select distinct
            subject_id, ftd_index,
            'phe_278.11' as code,
        from "dbt"."main_main"."eMERGEseq_stg_phecode" as p
        union all
    
        select distinct
            subject_id, ftd_index,
            'phe_272.13' as code,
        from "dbt"."main_main"."eMERGEseq_stg_phecode" as p
        union all
    
        select distinct
            subject_id, ftd_index,
            'phe_587' as code,
        from "dbt"."main_main"."eMERGEseq_stg_phecode" as p
        
    
),

unpivot_bmi as (

        
            select distinct
            subject_id, ftd_index,
            'weight' AS "code",
            from "dbt"."main_main"."eMERGEseq_stg_bmi" as bmi
            union all
        
            select distinct
            subject_id, ftd_index,
            'height' AS "code",
            from "dbt"."main_main"."eMERGEseq_stg_bmi" as bmi
            union all
        
            select distinct
            subject_id, ftd_index,
            'body_mass_index' AS "code",
            from "dbt"."main_main"."eMERGEseq_stg_bmi" as bmi
            
        
    ),
    
unpivot_bmi_case as (
    select 
    subject_id, ftd_index,
     CASE 
          WHEN code = 'weight'THEN 'LOINC:29463-7'
          WHEN code = 'height' THEN 'LOINC:8302-2'
          WHEN code = 'body_mass_index' THEN 'LOINC:39156-5'
          ELSE CONCAT('FTD_FLAG: unhandled code: ', code)
     END AS code
    from unpivot_bmi
    ),
    
union_data as (
    select * from unpivot_df as ud

    union all 
    
    select * from unpivot_bmi_case as ubc
)

    select distinct
    'sa' || '_' || md5('phs001616' || '|' || cast(coalesce(ud.subject_id, '') as text) || '|' || 'phs001616' || '|' || cast(coalesce(ud.code, '') as text))::text as "subjectassertion_id",
       ud.code::text as "external_id"
        from union_data as ud
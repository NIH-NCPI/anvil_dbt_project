



  





with lookup as(
    select 
        LOWER(variable_name) as code, 
    from "dbt"."main"."phecode_seed"),

     
unpivot_df as (

    
),

unpivot_bmi as (

        
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
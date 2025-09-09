

    with source as (
        select 
        CASE
            WHEN demographics.year_birth = 'NA' THEN null
            WHEN demographics.year_birth = '.' THEN null
            ELSE demographics.year_birth
        END::integer as "date_of_birth",
       'year_only'::text as "date_of_birth_type",
        CASE demographics.sex
            WHEN 'C46110' THEN 'female'
            WHEN 'C46109' THEN 'male'
            WHEN 'U' THEN 'unknown'
            WHEN '.' THEN 'unknown'
            WHEN 'NA' THEN 'unknown'
        END::text as "sex",       
         CASE demographics.sex
            WHEN 'C46110' THEN 'Female'
            WHEN 'C46109' THEN 'Male'
            WHEN 'U' THEN 'Unknown'
            WHEN '.' THEN 'Unknown'
            WHEN 'NA' THEN 'Unknown'
        END::text as "sex_display",
       CASE demographics.race
            WHEN 'C41259' THEN 'American Indian or Alaska Native'
            WHEN 'C41260' THEN 'Asian'
            WHEN 'C16352' THEN 'Black or African American'
            WHEN 'C41219' THEN 'Native Hawaiian or  Other Pacific Islander'
            WHEN 'C41261' THEN 'White'
            WHEN 'C17998' THEN 'unknown'
            WHEN 'C43234' THEN 'unknown'
            WHEN 'C13652' THEN 'unknown'
            WHEN '.' THEN 'unknown'
            WHEN 'NA' THEN 'unknown'
            WHEN 'OTHER' THEN 'other race'
       END::text as "race_display",
       CASE demographics.ethnicity
            WHEN 'C17459' THEN 'hispanic_or_latino'
            WHEN 'C41222' THEN 'not_hispanic_or_latino'
            WHEN 'C41221' THEN 'unknown'
            WHEN '.' THEN 'unknown'
            WHEN 'NA' THEN 'unknown'
        END::text as "ethnicity",
        CASE demographics.ethnicity
            WHEN 'C17459' THEN 'Hispanic or Latino'
            WHEN 'C41222' THEN 'Not Hispanic or Latino'
            WHEN 'C41221' THEN 'unknown'
            WHEN '.' THEN 'unknown'
            WHEN 'NA' THEN 'unknown'
        END::text as "ethnicity_display",
        NULL as "age_at_last_vital_status",
        NULL as "vital_status",
       'ap' || '_' || md5('phs001616' || '|' || cast(coalesce(subjectconsent.consent, '') as text))::text as "has_access_policy",
       'dm' || '_' || md5('phs001616' || '|' || cast(coalesce(demographics.subject_id, '') as text))::text as "id"
        from "dbt"."main_main"."eMERGEseq_stg_demographics" as demographics
        left join "dbt"."main_main"."eMERGEseq_stg_subjectconsent" as subjectconsent
        on demographics.subject_id = subjectconsent.subject_id
        
    )

select 
        * 
    from source
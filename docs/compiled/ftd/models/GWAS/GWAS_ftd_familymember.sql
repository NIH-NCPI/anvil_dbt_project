
    
    
      
    
    with 
    unpivot_df as (

        
            select distinct
                ftd_index, family_id,
                mother as "other_family_memb_id",
                subject_id as "proband_id",
                NULL as "sex",
                'mother' as "family_role",
            from "dbt"."main_main"."GWAS_stg_pedigree" as p
            WHERE mother IS NOT NULL 
            union all
        
            select distinct
                ftd_index, family_id,
                father as "other_family_memb_id",
                subject_id as "proband_id",
                NULL as "sex",
                'father' as "family_role",
            from "dbt"."main_main"."GWAS_stg_pedigree" as p
            WHERE father IS NOT NULL 
            union all
        
            select distinct
                ftd_index, family_id,
                mz_twin_id as "other_family_memb_id",
                subject_id as "proband_id",
                NULL as "sex",
                'mz_twin_id' as "family_role",
            from "dbt"."main_main"."GWAS_stg_pedigree" as p
            WHERE mz_twin_id IS NOT NULL 
            
        
    ),
    
   all_pedigree as (
   select distinct
        ftd_index,
        family_id,
        subject_id as "proband_id",
        subject_id,
        null as "other_family_memb_id",
        mother,
        father,
        mz_twin_id,
        p.sex,
        'CHILD' as "family_role",
    from "dbt"."main_main"."GWAS_stg_pedigree" as p
    where mother is not null
    and father is not null
    and mz_twin_id is not null
 union all
    select distinct
        p.ftd_index,
        p.family_id,
        proband_id,
        other_family_memb_id AS "subject_id",
        other_family_memb_id,
        NULL AS mother,
        NULL AS father,
        NULL AS mz_twin_id,
        p.sex,
        CASE p.sex
           WHEN 2 then 'MTH' 
           WHEN 1 THEN 'FTH'
        END::text as "family_role"
    from "dbt"."main_main"."GWAS_stg_pedigree" as p
    left join unpivot_df as u on u.proband_id = p.subject_id
              and u.family_id = p.family_id
    where mother is null
    and father is null
    and mz_twin_id is null
),
        source as (
        select distinct
        'sb' || '_' || md5('phs001584' || '|' || cast(coalesce(subjectconsent.subject_id, '') as text))::text as "family_member",
       family_role,
       'ap' || '_' || md5('phs001584' || '|' || cast(coalesce(subjectconsent.consent, '') as text))::text as "has_access_policy",
       'fm' || '_' || md5('phs001584' || '|' || cast(coalesce(pedigree.subject_id, '') as text) || '|' || 'phs001584' || '|' || cast(coalesce(pedigree.family_id, '') as text))::text as "id",
       'fy' || '_' || md5('phs001584' || '|' || cast(coalesce(pedigree.family_id, '') as text))::text as "family_id"
       from all_pedigree as pedigree
       left join "dbt"."main_main"."GWAS_stg_subjectconsent" as subjectconsent
        on pedigree.subject_id = subjectconsent.subject_id
    )

    select 
        * 
    from source
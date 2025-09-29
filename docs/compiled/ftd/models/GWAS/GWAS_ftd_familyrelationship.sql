
    
    
      
    
    with 
    unpivot_df as (

        
            select
                ftd_index, family_id,
                mother as "other_family_memb_id",
                subject_id as "proband_id",
                NULL as "sex",
                'mother' as "relationship",
            from "dbt"."main_main"."GWAS_stg_pedigree" as p
            WHERE mother IS NOT NULL 
            union all
        
            select
                ftd_index, family_id,
                father as "other_family_memb_id",
                subject_id as "proband_id",
                NULL as "sex",
                'father' as "relationship",
            from "dbt"."main_main"."GWAS_stg_pedigree" as p
            WHERE father IS NOT NULL 
            union all
        
            select
                ftd_index, family_id,
                mz_twin_id as "other_family_memb_id",
                subject_id as "proband_id",
                NULL as "sex",
                'mz_twin_id' as "relationship",
            from "dbt"."main_main"."GWAS_stg_pedigree" as p
            WHERE mz_twin_id IS NOT NULL 
            
        
    ),
    
   direct_relationship as (
   select
       p.ftd_index,
       p.family_id,
       proband_id as "family_member",
       other_family_memb_id as "other_family_member",
       CASE  
           WHEN relationship = 'mother' THEN 'KIN:032'
           WHEN relationship = 'father' THEN 'KIN:032'
           WHEN relationship = 'mz_twin_id' THEN 'KIN:010'
       END::text AS relationship_code
       from "dbt"."main_main"."GWAS_stg_pedigree" as p
    left join unpivot_df as u on u.proband_id = p.subject_id
       and u.family_id = p.family_id
    where mother is not null
    and father is not null
    and mz_twin_id is not null
),
    
   reverse_twin_relationship as (
   select -- reverse relationship for twins
       dr.ftd_index,
       dr.family_id,
       dr.other_family_member as "family_member",
       dr.family_member as "other_family_member",
       dr.relationship_code
    from direct_relationship as dr
    where relationship_code = 'KIN:010'
    )

    select DISTINCT
        'fr' || '_' || md5('phs001584' || '|' || cast(coalesce(family_id, '') as text) || '|' || 'phs001584' || '|' || cast(coalesce(family_member, '') as text) || '|' || 'phs001584' || '|' || cast(coalesce(other_family_member, '') as text) || '|' || 'phs001584' || '|' || cast(coalesce(relationship_code, '') as text))::text as "id",
        relationship_code, 
        'sb' || '_' || md5('phs001584' || '|' || cast(coalesce(family_member, '') as text))::text AS "family_member",
        'sb' || '_' || md5('phs001584' || '|' || cast(coalesce(other_family_member, '') as text))::text as "other_family_member",
        'ap' || '_' || md5('phs001584' || '|' || cast(coalesce(subjectconsent.consent, '') as text))::text as "has_access_policy"

    from (
        select distinct * from direct_relationship 
    
    union all
    
        select distinct * from reverse_twin_relationship) as combined_relationship
    left join "dbt"."main_main"."GWAS_stg_subjectconsent" as subjectconsent
    on subjectconsent.subject_id = combined_relationship.family_member
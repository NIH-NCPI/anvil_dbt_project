

 with source as (
        select DISTINCT
        subject_id AS other_family_member, 
        paternal_id AS subject_id,
        consent_id,
        'KIN:028' AS relationship_code,
        family_id
        from "dbt"."main_main"."cser_stg_subject" as subject WHERE paternal_id is not null

        UNION 

        select DISTINCT
        subject_id AS other_family_member, 
        maternal_id AS subject_id,
        consent_id,
        'KIN:027' AS relationship_code,
         family_id
        from "dbt"."main_main"."cser_stg_subject" as subject WHERE maternal_id is not null
     
        UNION 
        select DISTINCT        
        maternal_id AS other_family_member, 
        subject_id,
        consent_id,
        'KIN:032' AS relationship_code,
         family_id
        from "dbt"."main_main"."cser_stg_subject" as subject WHERE maternal_id is not null
     
        UNION 
        select DISTINCT        
        paternal_id AS other_family_member, 
        subject_id,
        consent_id,
        'KIN:032' AS relationship_code,
         family_id
        from "dbt"."main_main"."cser_stg_subject" as subject WHERE paternal_id is not null
    )

    select distinct
  'sb' || '_' || md5('cser' || '|' || cast(coalesce(subject_id, '') as text))::text as "family_member",
 'sb' || '_' || md5('cser' || '|' || cast(coalesce(other_family_member, '') as text))::text as "other_family_member",
  relationship_code,
    'ap' || '_' || md5('cser' || '|' || cast(coalesce(consent_id, '') as text))::text as "has_access_policy",
 'fr' || '_' || md5('cser' || '|' || cast(coalesce(family_id, '') as text) || '|' || 'cser' || '|' || cast(coalesce(subject_id, '') as text) || '|' || 'cser' || '|' || cast(coalesce(other_family_member, '') as text) || '|' || 'cser' || '|' || cast(coalesce(relationship_code, '') as text))::text as "id"
 from source
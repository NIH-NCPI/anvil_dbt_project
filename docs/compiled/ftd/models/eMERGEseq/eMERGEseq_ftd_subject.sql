

    with source as (
        select DISTINCT
         CASE 
            WHEN subjectconsent.consent != 0 THEN 'participant'
            ELSE 'non_participant'
        END::text as subject_type,
        NULL as "organism_type",
       'ap' || '_' || md5('phs001616' || '|' || cast(coalesce(subjectconsent.consent, '') as text))::text as "has_access_policy",
       'sb' || '_' || md5('phs001616' || '|' || cast(coalesce(subjectconsent.subject_id, '') as text))::text as "id",
       'dm' || '_' || md5('phs001616' || '|' || cast(coalesce(demographics.subject_id, '') as text))::text as "has_demographics_id"
        from "dbt"."main_main"."eMERGEseq_stg_subjectconsent" as subjectconsent
        left join "dbt"."main_main"."eMERGEseq_stg_demographics" as demographics
        on subjectconsent.subject_id = demographics.subject_id
    )

    select 
        * 
    from source